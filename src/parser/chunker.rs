use regex::Regex;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Chunk {
    pub content: String,
    pub headers: Vec<String>,
}

pub struct ChunkerOptions {
    pub max_size: usize,
    pub min_size: usize,
    pub overlap: usize,
}

impl Default for ChunkerOptions {
    fn default() -> Self {
        Self {
            max_size: 1000,
            min_size: 100,
            overlap: 50,
        }
    }
}

pub struct MarkdownChunker;

impl MarkdownChunker {
    /// Chunks a markdown string structurally by headers.
    /// If a section exceeds `options.max_size`, it will be further split by paragraphs.
    pub fn chunk(markdown: &str, options: &ChunkerOptions) -> Vec<Chunk> {
        let mut sections = Vec::new();
        let atx_header_re = Regex::new(r"^(#{1,6})\s+(.*?)(?:\s+#+)?$").expect("Valid regex");
        
        let mut current_headers: Vec<String> = Vec::new();
        let mut current_section_content = String::new();

        let lines: Vec<&str> = markdown.lines().collect();
        let mut i = 0;
        
        while i < lines.len() {
            let line = lines[i];
            let mut is_header = false;
            let mut level = 0;
            let mut title = String::new();

            // Check for ATX header
            if let Some(caps) = atx_header_re.captures(line) {
                is_header = true;
                level = caps[1].len();
                title = caps[2].trim().to_string();
            } 
            // Check for Setext header
            else if i + 1 < lines.len() && !line.trim().is_empty() {
                let next_line = lines[i+1].trim();
                if !next_line.is_empty() && next_line.chars().all(|c| c == '=') && next_line.len() >= 3 {
                    is_header = true;
                    level = 1;
                    title = line.trim().to_string();
                } else if !next_line.is_empty() && next_line.chars().all(|c| c == '-') && next_line.len() >= 3 {
                    is_header = true;
                    level = 2;
                    title = line.trim().to_string();
                }
            }

            if is_header {
                // Save previous section if not empty
                if !current_section_content.trim().is_empty() {
                    let breadcrumb: Vec<String> = current_headers.iter()
                        .filter(|h| !h.is_empty())
                        .cloned()
                        .collect();
                    sections.push((breadcrumb, current_section_content.trim().to_string()));
                    current_section_content.clear();
                }

                // Update headers breadcrumb
                if level <= current_headers.len() {
                    current_headers.truncate(level - 1);
                }
                while current_headers.len() < level - 1 {
                    current_headers.push(String::new());
                }
                current_headers.push(title);

                // If Setext, we must consume TWO lines
                if !atx_header_re.is_match(line) {
                    current_section_content.push_str(line);
                    current_section_content.push('\n');
                    current_section_content.push_str(lines[i+1]);
                    current_section_content.push('\n');
                    i += 2;
                    continue;
                }
            }

            current_section_content.push_str(line);
            current_section_content.push('\n');
            i += 1;
        }

        if !current_section_content.trim().is_empty() {
            let breadcrumb: Vec<String> = current_headers.iter()
                .filter(|h| !h.is_empty())
                .cloned()
                .collect();
            sections.push((breadcrumb, current_section_content.trim().to_string()));
        }

        // 2. Sub-splitting and merging sections
        let mut chunks = Vec::new();
        let mut pending_content = String::new();
        let mut pending_headers = Vec::new();

        for (headers, content) in sections {
            if content.len() > options.max_size {
                // Flush pending
                if !pending_content.is_empty() {
                    chunks.push(Chunk { content: pending_content.trim().to_string(), headers: pending_headers.clone() });
                    pending_content.clear();
                }
                // Split large content
                chunks.extend(Self::split_large_content(&content, &headers, options));
            } else {
                if pending_content.is_empty() {
                    pending_content = content;
                    pending_headers = headers;
                } else if pending_content.len() + content.len() + 2 <= options.max_size {
                    pending_content.push_str("\n\n");
                    pending_content.push_str(&content);
                    pending_headers = headers; // Update to the most recent headers for context
                } else {
                    chunks.push(Chunk { content: pending_content.trim().to_string(), headers: pending_headers.clone() });
                    pending_content = content;
                    pending_headers = headers;
                }
            }
        }

        if !pending_content.trim().is_empty() {
            chunks.push(Chunk { content: pending_content.trim().to_string(), headers: pending_headers });
        }

        chunks
    }

    fn split_large_content(content: &str, headers: &[String], options: &ChunkerOptions) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        let paragraphs: Vec<&str> = content.split("\n\n").collect();
        let mut sub_content = String::new();

        for p in paragraphs {
            let p = p.trim();
            if p.is_empty() { continue; }

            if p.len() <= options.max_size {
                if sub_content.len() + p.len() + 2 > options.max_size && !sub_content.is_empty() {
                    chunks.push(Chunk { content: sub_content.trim().to_string(), headers: headers.to_vec() });
                    sub_content.clear();
                }
                sub_content.push_str(p);
                sub_content.push_str("\n\n");
            } else {
                // Paragraph itself is too large, split by characters
                if !sub_content.trim().is_empty() {
                    chunks.push(Chunk { content: sub_content.trim().to_string(), headers: headers.to_vec() });
                    sub_content.clear();
                }
                
                let mut p_str = p.to_string();
                while p_str.len() > options.max_size {
                    let split_idx = p_str.char_indices()
                        .map(|(i, _)| i)
                        .filter(|&i| i <= options.max_size)
                        .last()
                        .unwrap_or(options.max_size);
                    
                    chunks.push(Chunk { content: p_str[..split_idx].trim().to_string(), headers: headers.to_vec() });
                    p_str = p_str[split_idx..].to_string();
                }
                if !p_str.trim().is_empty() {
                    sub_content.push_str(&p_str);
                    sub_content.push_str("\n\n");
                }
            }
        }

        if !sub_content.trim().is_empty() {
            chunks.push(Chunk { content: sub_content.trim().to_string(), headers: headers.to_vec() });
        }

        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_structural_chunking() {
        let markdown = r#"
# Title
Intro text

## Section 1
Content of section 1
"#;
        let options = ChunkerOptions {
            max_size: 30, // Force split
            min_size: 10,
            overlap: 0,
        };
        let chunks = MarkdownChunker::chunk(markdown, &options);

        assert!(chunks.len() >= 2);
        assert!(chunks[0].content.contains("# Title"));
        assert!(chunks[1].content.contains("## Section 1"));
    }

    #[test]
    fn test_setext_headers() {
        let markdown = "Title\n=====\nBody content\n\nSubtitle\n--------\nMore content";
        let options = ChunkerOptions::default();
        let chunks = MarkdownChunker::chunk(markdown, &options);

        assert_eq!(chunks.len(), 1); // Merged because small
        assert!(chunks[0].content.contains("Title\n====="));
        assert!(chunks[0].content.contains("Subtitle\n--------"));
        assert_eq!(chunks[0].headers, vec!["Title".to_string(), "Subtitle".to_string()]);
    }

    #[test]
    fn test_size_splitting() {
        let markdown = "This is a very long text that should be split into multiple chunks because it exceeds the maximum size limit set in options.";
        let options = ChunkerOptions {
            max_size: 40,
            min_size: 10,
            overlap: 0,
        };
        let chunks = MarkdownChunker::chunk(markdown, &options);

        assert!(chunks.len() > 1);
    }
}
