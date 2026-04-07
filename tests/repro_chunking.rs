use rust_eventbus::parser::chunker::{MarkdownChunker, ChunkerOptions};

#[test]
fn test_small_chunk_repro() {
    let markdown = r#"
# Introduction
## How to use this book
That said, there is no wrong way to read this book. Read it however you feel helps you best.

# Next Big Section
"# .to_string() + &"A".repeat(1000);

    let options = ChunkerOptions {
        max_size: 500,
        min_size: 300,
        overlap: 100,
    };

    let chunks = MarkdownChunker::chunk(&markdown, &options);

    for (i, chunk) in chunks.iter().enumerate() {
        println!("Chunk {}: len={}, headers={:?}", i, chunk.content.len(), chunk.headers);
        println!("Content: {}\n", chunk.content);
    }

    // The first chunk should be at least min_size if possible
    assert!(chunks[0].content.len() >= options.min_size || chunks.len() == 1);
}
