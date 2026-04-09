# Ingestion Service API

The web server crawls websites, chunks content, generates embeddings, and stores everything in PostgreSQL with pgvector. All data is scoped by `project_id`.

## Running

```bash
DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example web_server
```

The service creates tables automatically on startup. Requires PostgreSQL with the `vector` extension installed.

## Endpoints

### `GET /crawl?url=<url>&project_id=<id>`

Starts crawling the given URL and discovers same-domain links. Returns an SSE stream of node/edge events as they are processed.

```bash
curl -N "http://localhost:3010/crawl?url=https://example.com&project_id=proj_123"
```

The response includes `X-Accel-Buffering: no` header for nginx compatibility (disables proxy buffering).

SSE events are JSON-serialized `AppEvent` variants or progress updates:

```jsonl
data: {"type":"progress","total_nodes":1,"embedded_nodes":0,"ingested_urls":1,"requested_urls":1}
data: {"type":"Graph","data":{"type":"NodeCreated","data":{"id":"https://example.com#chunk-0","metadata":{...}}}}
data: {"type":"Graph","data":{"type":"EdgeAdded","data":{"from":"https://example.com#chunk-0","to":"https://example.com","relation":"part_of","weight":1.0}}}
data: {"type":"progress","total_nodes":1,"embedded_nodes":0,"ingested_urls":1,"requested_urls":1}
data: {"type":"Embedding","data":{"EmbeddingExtracted":{"id":"https://example.com#chunk-0","embedding":[...]}}}
data: {"type":"progress","total_nodes":1,"embedded_nodes":1,"ingested_urls":1,"requested_urls":1}
```

**Progress event fields:**
- `type`: Always `"progress"`
- `total_nodes`: Total nodes created (chunks)
- `embedded_nodes`: Nodes that have embeddings computed
- `ingested_urls`: Pages successfully crawled
- `requested_urls`: Total URLs discovered and queued

The stream closes automatically when all discovered pages are crawled and all embeddings are computed (or after 30s of inactivity).

### `POST /search`

Hybrid search combining vector similarity (cosine) and full-text ranking (BM25-style), scoped to a project.

```bash
curl -X POST http://localhost:3010/search \
  -H "Content-Type: application/json" \
  -d '{"query": "how to deploy", "project_id": "proj_123"}'
```

Response:

```json
[
  {
    "vector_score": 0.82,
    "text_score": 0.15,
    "score": 0.619,
    "id": "https://example.com/docs/deploy#chunk-2",
    "page_url": "https://example.com/docs/deploy",
    "title": "Deployment Guide",
    "section": "Getting Started > Deploy",
    "content": "To deploy the application...",
    "chunk_index": 2
  }
]
```

### `POST /embeddings`

Generate an embedding for arbitrary text (useful for the consuming service).

```bash
curl -X POST http://localhost:3010/embeddings \
  -H "Content-Type: application/json" \
  -d '{"text": "some text to embed"}'
```

## Database Schema

```sql
-- Pages crawled per project
CREATE TABLE pages (
    url         TEXT NOT NULL,
    project_id  TEXT NOT NULL,
    title       TEXT,
    crawled_at  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (url, project_id)
);

-- Chunked content with embeddings (bge-small-en-v1.5, 384 dimensions)
CREATE TABLE chunks (
    id          TEXT NOT NULL,        -- "https://example.com/path#chunk-3"
    project_id  TEXT NOT NULL,
    page_url    TEXT NOT NULL,
    chunk_index INT NOT NULL,         -- reading order within the page
    section     TEXT,                  -- breadcrumb headers, e.g. "Setup > Config"
    content     TEXT NOT NULL,
    embedding   vector(384),
    created_at  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (id, project_id),
    UNIQUE (page_url, project_id, chunk_index)
);

-- Links between pages (optional, for authority scoring)
CREATE TABLE page_links (
    from_url    TEXT NOT NULL,
    to_url      TEXT NOT NULL,
    project_id  TEXT NOT NULL,
    PRIMARY KEY (from_url, to_url, project_id)
);
```

## Querying from the Consuming Service

### Hybrid search

```sql
SELECT
    c.id,
    c.page_url,
    COALESCE(p.title, c.page_url) AS title,
    COALESCE(c.section, '') AS section,
    c.content,
    c.chunk_index,
    (1 - (c.embedding <=> $1))::real AS vector_score,
    ts_rank(to_tsvector('english', c.content), plainto_tsquery('english', $2)) AS text_score,
    (1 - (c.embedding <=> $1))::real * 0.7
        + ts_rank(to_tsvector('english', c.content), plainto_tsquery('english', $2)) * 0.3
        AS score
FROM chunks c
JOIN pages p ON c.page_url = p.url AND c.project_id = p.project_id
WHERE c.project_id = $3
  AND c.embedding IS NOT NULL
ORDER BY score DESC
LIMIT 20;
```

`$1` is the query embedding (384-dim vector), `$2` is the raw query text, `$3` is the project ID.

### Context expansion (grab surrounding chunks)

When a chunk scores highly, fetch its neighbors for a coherent passage:

```sql
SELECT content, section, chunk_index
FROM chunks
WHERE page_url = $1
  AND project_id = $2
  AND chunk_index BETWEEN $3 - 2 AND $3 + 2
ORDER BY chunk_index;
```

### Get all chunks for a page

```sql
SELECT id, chunk_index, section, content,
       (embedding IS NOT NULL) AS has_embedding
FROM chunks
WHERE page_url = $1 AND project_id = $2
ORDER BY chunk_index;
```

### Authority scoring (pages with most inbound links)

```sql
SELECT to_url, COUNT(*) AS inbound
FROM page_links
WHERE project_id = $1
GROUP BY to_url
ORDER BY inbound DESC
LIMIT 10;
```

Use this to boost chunks from high-authority pages in your reranking logic.

## Tuning

- **Score weights**: the `0.7` / `0.3` split between vector and text scores is a starting point. Tune based on your content — technical docs may benefit from higher text weight, conversational content from higher vector weight.
- **Embedding model**: uses `bge-small-en-v1.5` (384 dims). To switch models, update the model download in the service and change the `vector(384)` column dimension.
- **HNSW index**: the default index works well up to ~1M chunks. For larger datasets, consider tuning `m` and `ef_construction` parameters.
