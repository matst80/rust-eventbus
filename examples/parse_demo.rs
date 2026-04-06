use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use scraper::Html;
use rust_eventbus::parser::Extractor;

fn main() -> Result<()> {
    // 1. Setup paths
    let input_dir = "examples/inputs";
    let output_dir = "examples/outputs";

    if !Path::new(input_dir).exists() {
        anyhow::bail!("Input directory not found: {}. Please run from the project root.", input_dir);
    }

    // Ensure output directory exists
    fs::create_dir_all(output_dir)?;

    // 2. Iterate over input files
    let entries = fs::read_dir(input_dir)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?;

    for path in entries {
        if path.is_dir() || path.extension().and_then(|s| s.to_str()) != Some("html") {
            continue;
        }

        let file_name = path.file_name().unwrap().to_str().unwrap();
        let output_file_name = format!("{}.md", file_name);
        let output_path = PathBuf::from(output_dir).join(output_file_name);

        println!("\n>>> Processing input: {}", file_name);
        let html_content = fs::read_to_string(&path)
            .context(format!("Failed to read input HTML: {:?}", path))?;

        // Parse using Extractor
        let html = Html::parse_document(&html_content);
        let url = format!("https://example.com/demo/{}", file_name); 
        
        println!("  - extracting title...");
        let title = html.select(&scraper::Selector::parse("title").unwrap())
            .next()
            .map(|el| el.text().collect::<String>())
            .unwrap_or_else(|| "No Title".to_string());

        println!("  - extracting links...");
        let links = Extractor::extract_links(&html, &url);
        
        println!("  - extracting metadata...");
        let metadata = Extractor::extract_metadata(&html);
        
        println!("  - extracting structured data...");
        let structured_data = Extractor::extract_structured_data(&html);
        
        println!("  - converting to markdown (LLM friendly)...");
        // Demonstrate dynamic ignore patterns
        let extra_excludes = ["#coiOverlay", ".ex-vat","button","#cookie-information-template-wrapper"];
        let markdown = Extractor::to_markdown(&html_content,"https://example.com", &extra_excludes);

        // 3. Save Markdown to file
        println!("  - saving markdown to: {:?}", output_path);
        let mut final_markdown = format!("# {}\n\n", title);
        final_markdown.push_str(&format!("Source: {}\n\n", url));
        final_markdown.push_str("## Metadata\n\n");
        for (k, v) in &metadata {
            if !v.trim().is_empty() {
                final_markdown.push_str(&format!("- **{}**: {}\n", k, v.trim()));
            }
        }
        final_markdown.push_str("\n---\n\n");
        final_markdown.push_str(&markdown);

        fs::write(&output_path, final_markdown)?;

        // Summary
        println!("  - SUCCESS: Title: {}", title);
        println!("  - Links: {}, Metadata: {}, Structured blocks: {}", 
            links.len(), metadata.len(), structured_data.len());
    }

    println!("\nALL DONE. See examples/outputs/ for results.");

    Ok(())
}
