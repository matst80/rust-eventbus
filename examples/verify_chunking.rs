use std::fs;
use std::path::Path;
use rust_eventbus::parser::chunker::{MarkdownChunker, ChunkerOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_path = "examples/outputs/docs.html.md";
    let output_dir = "examples/outputs/chunks";

    // Read markdown
    let markdown = fs::read_to_string(input_path)?;

    // Chunker options
    let options = ChunkerOptions {
        max_size: 1000,
        min_size: 100,
        overlap: 50,
    };

    // Create output directory
    if !Path::new(output_dir).exists() {
        fs::create_dir_all(output_dir)?;
    }

    // Clean old chunks
    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::remove_file(entry.path())?;
        }
    }

    // Chunk markdown
    let chunks = MarkdownChunker::chunk(&markdown, &options);

    println!("Total chunks: {}", chunks.len());

    // Write chunks to files
    for (i, chunk) in chunks.iter().enumerate() {
        let chunk_file = format!("{}/chunk_{:03}.md", output_dir, i);
        let mut content = format!("Headers: {:?}\n---\n", chunk.headers);
        content.push_str(&chunk.content);
        fs::write(&chunk_file, content)?;
    }

    println!("Chunks written to {}", output_dir);

    Ok(())
}
