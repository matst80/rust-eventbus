use anyhow::Result;
use pgvector::Vector;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPool, Row};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub vector_score: f32,
    pub text_score: f32,
    pub score: f32,
    pub id: String,
    pub page_url: String,
    pub title: String,
    pub section: String,
    pub content: String,
    pub chunk_index: i32,
}

pub struct PersistenceService {
    pool: Arc<PgPool>,
    embed_fn: Arc<dyn Fn(&str) -> Result<Vec<f32>> + Send + Sync>,
}

impl PersistenceService {
    pub fn new(
        pool: Arc<PgPool>,
        embed_fn: Arc<dyn Fn(&str) -> Result<Vec<f32>> + Send + Sync>,
    ) -> Self {
        Self { pool, embed_fn }
    }

    pub async fn init_schema(&self) -> Result<()> {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
            .execute(&*self.pool)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS bge_pages (
                url         TEXT NOT NULL,
                project_id  TEXT NOT NULL,
                title       TEXT,
                crawled_at  TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (url, project_id)
            )",
        )
        .execute(&*self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS bge_chunks (
                id          TEXT NOT NULL,
                project_id  TEXT NOT NULL,
                page_url    TEXT NOT NULL,
                chunk_index INT NOT NULL,
                section     TEXT,
                content     TEXT NOT NULL,
                embedding   vector(384),
                created_at  TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (id, project_id),
                UNIQUE (page_url, project_id, chunk_index)
            )",
        )
        .execute(&*self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS bge_page_links (
                from_url    TEXT NOT NULL,
                to_url      TEXT NOT NULL,
                project_id  TEXT NOT NULL,
                PRIMARY KEY (from_url, to_url, project_id)
            )",
        )
        .execute(&*self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_bge_chunks_embedding
             ON bge_chunks USING hnsw (embedding vector_cosine_ops)",
        )
        .execute(&*self.pool)
        .await
        .ok();

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_bge_chunks_project ON bge_chunks (project_id)")
            .execute(&*self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_bge_chunks_page
             ON bge_chunks (page_url, project_id, chunk_index)",
        )
        .execute(&*self.pool)
        .await?;

        Ok(())
    }

    pub async fn upsert_page(&self, url: &str, project_id: &str, title: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO bge_pages (url, project_id, title) VALUES ($1, $2, $3)
             ON CONFLICT (url, project_id) DO UPDATE SET title = $3",
        )
        .bind(url)
        .bind(project_id)
        .bind(title)
        .execute(&*self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_links_batch(&self, links: Vec<(String, String, String)>) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }

        let mut query =
            "INSERT INTO bge_page_links (from_url, to_url, project_id) VALUES ".to_string();

        for (i, _) in links.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("(${}, ${}, ${})", i * 3 + 1, i * 3 + 2, i * 3 + 3));
        }
        query.push_str(" ON CONFLICT DO NOTHING");

        let mut executor = sqlx::query(&query);
        for (from_url, to_url, project_id) in links {
            executor = executor.bind(from_url).bind(to_url).bind(project_id);
        }
        executor.execute(&*self.pool).await?;
        Ok(())
    }

    pub async fn insert_chunks_batch(
        &self,
        chunks: Vec<(String, String, String, i32, Option<String>, String)>,
    ) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        let mut query = "INSERT INTO bge_chunks (id, project_id, page_url, chunk_index, section, content) VALUES ".to_string();

        for (i, _) in chunks.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${})",
                i * 6 + 1,
                i * 6 + 2,
                i * 6 + 3,
                i * 6 + 4,
                i * 6 + 5,
                i * 6 + 6
            ));
        }
        query.push_str(" ON CONFLICT (id, project_id) DO NOTHING");

        let mut executor = sqlx::query(&query);
        for (id, project_id, page_url, chunk_index, section, content) in chunks {
            executor = executor
                .bind(id)
                .bind(project_id)
                .bind(page_url)
                .bind(chunk_index)
                .bind(section)
                .bind(content);
        }
        executor.execute(&*self.pool).await?;
        Ok(())
    }

    pub async fn update_embedding(&self, id: &str, embedding: Vec<f32>) -> Result<u64> {
        let vec = Vector::from(embedding);
        let result = sqlx::query("UPDATE bge_chunks SET embedding = $1 WHERE id = $2")
            .bind(vec)
            .bind(id)
            .execute(&*self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    pub async fn update_embeddings_batch(&self, embeddings: Vec<(String, Vec<f32>)>) -> Result<()> {
        if embeddings.is_empty() {
            return Ok(());
        }

        // We use a temporary table or a VALUES join to update multiple rows in one query.
        // For simplicity and to avoid complex SQL for now, we'll use a transaction with multiple updates
        // but it's still better than individual await calls in the main loop.
        let mut tx = self.pool.begin().await?;

        for (id, embedding) in embeddings {
            let vec = Vector::from(embedding);
            sqlx::query("UPDATE bge_chunks SET embedding = $1 WHERE id = $2")
                .bind(vec)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn search(&self, query: &str, project_id: &str) -> Result<Vec<SearchResult>> {
        tracing::debug!("Searching for '{}' in project '{}'", query, project_id);

        let embed_fn = Arc::clone(&self.embed_fn);
        let query_owned = query.to_string();
        let raw_vec = tokio::task::spawn_blocking(move || embed_fn(&query_owned))
            .await
            .map_err(|e| anyhow::anyhow!("Query embedding task failed: {}", e))??;
        tracing::debug!("Query embedding length: {}", raw_vec.len());

        if raw_vec.is_empty() {
            return Err(anyhow::anyhow!("Query embedding returned an empty vector"));
        }

        let query_vec = Vector::from(raw_vec);

        let rows = sqlx::query(
            "SELECT
                c.id,
                c.page_url,
                COALESCE(p.title, c.page_url) AS title,
                COALESCE(c.section, '') AS section,
                c.content,
                c.chunk_index,
                (1 - (c.embedding <=> $1))::real AS vector_score,
                ts_rank(to_tsvector('english', c.content), plainto_tsquery('english', $2)) AS text_score
            FROM bge_chunks c
            JOIN bge_pages p ON c.page_url = p.url AND c.project_id = p.project_id
            WHERE c.project_id = $3
              AND c.embedding IS NOT NULL
            ORDER BY
                (1 - (c.embedding <=> $1))::real * 0.7
                + ts_rank(to_tsvector('english', c.content), plainto_tsquery('english', $2)) * 0.3
                DESC
            LIMIT 20",
        )
        .bind(&query_vec)
        .bind(query)
        .bind(project_id)
        .fetch_all(&*self.pool)
        .await?;

        let results = rows
            .iter()
            .map(|row| {
                let vector_score: f32 = row.get("vector_score");
                let text_score: f32 = row.get("text_score");
                SearchResult {
                    vector_score,
                    text_score,
                    score: vector_score * 0.7 + text_score * 0.3,
                    id: row.get("id"),
                    page_url: row.get("page_url"),
                    title: row.get("title"),
                    section: row.get("section"),
                    content: row.get("content"),
                    chunk_index: row.get("chunk_index"),
                }
            })
            .collect();

        Ok(results)
    }
}
