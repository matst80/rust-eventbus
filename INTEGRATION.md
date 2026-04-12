# Integration Guide: Crawler & EventBus Service

This guide explains how to integrate with the deployed Crawler service (`crawler-web`) in the `iamu` namespace.

## Data Models

### Chunk Structure
The primary unit of content extraction. Chunks are structurally split by headers (Markdown/HTML) and paragraphs.

```json
{
  "content": "The actual text content of the chunk.",
  "headers": ["Top Level Header", "Sub-header", "Section Title"]
}
```

### Search Result
Returned by the `/search` endpoint. Includes both vector similarity and full-text search scores.

```json
{
  "id": "https://example.com/page#chunk-0",
  "page_url": "https://example.com/page",
  "title": "Page Title",
  "section": "Header > Subheader",
  "content": "Chunk content...",
  "chunk_index": 0,
  "score": 0.85,
  "vector_score": 0.92,
  "text_score": 0.12
}
```

### Crawl Progress (SSE)
Streamed during a `/crawl` operation to provide real-time status.

```json
{
  "url": "https://example.com/current-page",
  "title": "Current Page Title",
  "chunk_count": 12,
  "links_found": 45,
  "total_processed": 5,
  "is_done": false
}
```

---

## API Endpoints

### 1. Crawl & Ingest
Starts a recursive crawl of a domain. Chunks are automatically extracted, embedded, and persisted.

**Endpoint:** `GET /crawl`  
**Query Parameters:**
- `url`: The starting URL.
- `project_id`: A unique identifier for the dataset (e.g., `docs-v1`).
- `max_crawls` (optional, default 50): Max pages to visit.
- `max_chunks` (optional, default 50): Max chunks per page.

**Response:** Server-Sent Events (SSE) streaming `CrawlProgress` objects.

```bash
curl -N "http://<host>:<port>/crawl?url=https://go.dev/doc/contribute&project_id=golang"
```

### 2. Semantic Search
Performs a hybrid (Vector + BM25) search across ingested chunks.

**Endpoint:** `POST /search`  
**Body:**
```json
{
  "query": "How do I submit a code review?",
  "project_id": "golang"
}
```

### 3. Generate Embeddings
Direct access to the BGE-small embedding model (384 dimensions).

**Endpoint:** `POST /embeddings`  
**Body:** 
```json
{
  "text": "The text you want to embed."
}
```
**Response:** 
```json
{
  "embedding": [0.012, -0.045, ...]
}
```

---

## Operational Notes
- **Domain Locking:** The crawler stays within the domain of the starting URL.
- **Deduplication:** URLs are normalized (fragments and queries removed) to avoid duplicate processing.
- **Normalization:** Chunks always include their hierarchical breadcrumbs in the `section` or `headers` fields to preserve context.
