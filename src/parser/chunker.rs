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
            max_size: 1500,
            min_size: 300,
            overlap: 20,
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
                let next_line = lines[i + 1].trim();
                if !next_line.is_empty()
                    && next_line.chars().all(|c| c == '=')
                    && next_line.len() >= 3
                {
                    is_header = true;
                    level = 1;
                    title = line.trim().to_string();
                } else if !next_line.is_empty()
                    && next_line.chars().all(|c| c == '-')
                    && next_line.len() >= 3
                {
                    is_header = true;
                    level = 2;
                    title = line.trim().to_string();
                }
            }

            if is_header {
                // Save previous section if not empty
                if !current_section_content.trim().is_empty() {
                    let breadcrumb: Vec<String> = current_headers
                        .iter()
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
                    current_section_content.push_str(lines[i + 1]);
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
            let breadcrumb: Vec<String> = current_headers
                .iter()
                .filter(|h| !h.is_empty())
                .cloned()
                .collect();
            sections.push((breadcrumb, current_section_content.trim().to_string()));
        }

        // 2. Combine into atoms (headers + paragraphs)
        let mut atoms = Vec::new();
        for (headers, content) in sections {
            for p in content.split("\n\n") {
                let p = p.trim();
                if p.is_empty() {
                    continue;
                }
                atoms.push((headers.clone(), p.to_string()));
            }
        }

        // 3. Group atoms into chunks with overlap
        let mut chunks = Vec::new();
        let mut current_content = String::new();
        let mut current_headers = Vec::new();

        for (headers, p) in atoms {
            if !current_content.is_empty() && current_content.len() + p.len() + 2 > options.max_size {
                // Flush current chunk
                chunks.push(Chunk {
                    content: current_content.trim().to_string(),
                    headers: current_headers.clone(),
                });

                // Handle overlap: skip if it's a clean paragraph break (blank line)
                // The user requested no overlap on blank lines. Paragraphs are separated by blank lines.
                current_content.clear();
            }

            current_headers = headers;
            if current_content.is_empty() {
                // First atom
            } else {
                current_content.push_str("\n\n");
            }
            current_content.push_str(&p);

            // If a single atom is STILL too large, split it by characters
            while current_content.len() > options.max_size {
                let mut split_search_len = options.max_size.min(current_content.len());
                while !current_content.is_char_boundary(split_search_len) {
                    split_search_len -= 1;
                }

                let mut is_blank_line = false;
                let split_idx = if let Some(idx) = current_content[..split_search_len].rfind("\n\n") {
                    is_blank_line = true;
                    idx
                } else if let Some(idx) = current_content[..split_search_len].rfind('\n') {
                    idx
                } else if let Some(idx) = current_content[..split_search_len].rfind(' ') {
                    idx
                } else {
                    split_search_len
                };

                let split_idx = if split_idx == 0 {
                    split_search_len
                } else {
                    split_idx
                };

                chunks.push(Chunk {
                    content: current_content[..split_idx].trim().to_string(),
                    headers: current_headers.clone(),
                });

                let overlap = if is_blank_line { 0 } else { options.overlap };
                let overlap_start = split_idx.saturating_sub(overlap);
                let boundary = current_content
                    .char_indices()
                    .map(|(i, _)| i)
                    .find(|&i| i >= overlap_start)
                    .unwrap_or(current_content.len());
                current_content = current_content[boundary..].to_string();
            }
        }

        if !current_content.trim().is_empty() {
            chunks.push(Chunk {
                content: current_content.trim().to_string(),
                headers: current_headers,
            });
        }

        // 4. Merge chunks that are still too small (min_size check)
        let mut merged_chunks: Vec<Chunk> = Vec::new();
        for chunk in chunks {
            if let Some(last) = merged_chunks.last_mut() {
                if last.content.len() < options.min_size {
                    last.content.push_str("\n\n");
                    last.content.push_str(&chunk.content);
                    last.headers = chunk.headers; // Prefer more recent headers for context
                } else {
                    merged_chunks.push(chunk);
                }
            } else {
                merged_chunks.push(chunk);
            }
        }

        // Backward merge for the last chunk if it's still too small
        if merged_chunks.len() > 1 {
            let last_idx = merged_chunks.len() - 1;
            if merged_chunks[last_idx].content.len() < options.min_size {
                let last = merged_chunks.remove(last_idx);
                let prev = &mut merged_chunks[last_idx - 1];
                if prev.content.len() + last.content.len() + 2 <= options.max_size * 2 {
                    prev.content.push_str("\n\n");
                    prev.content.push_str(&last.content);
                } else {
                    // If merging would make it way too large, keep it but we did our best
                    merged_chunks.push(last);
                }
            }
        }

        merged_chunks
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
        assert_eq!(
            chunks[0].headers,
            vec!["Title".to_string(), "Subtitle".to_string()]
        );
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

    #[test]
    fn test_chunk_preference() {
        let options = ChunkerOptions {
            max_size: 40,
            min_size: 10,
            overlap: 0,
        };

        // Case 1: Preference for \n\n
        let markdown = "Paragraph 1 that is quite long indeed.\n\nParagraph 2 that is also long.";
        let chunks = MarkdownChunker::chunk(markdown, &options);
        assert!(chunks[0].content.contains("Paragraph 1"));
        assert!(!chunks[0].content.contains("Paragraph 2"));

        // Case 2: Preference for \n
        let markdown = "Line 1 is long enough to fit.\nLine 2 is also here.";
        let chunks = MarkdownChunker::chunk(markdown, &options);
        assert_eq!(chunks[0].content, "Line 1 is long enough to fit.");

        // Case 3: Preference for space
        let markdown = "This is a sentence that will be split at a space.";
        let chunks = MarkdownChunker::chunk(markdown, &options);
        assert_eq!(chunks[0].content, "This is a sentence that will be split");
    }

    #[test]
    fn test_chunk_overlap_skip() {
        let options = ChunkerOptions {
            max_size: 40,
            min_size: 10,
            overlap: 10,
        };

        // Blank line split -> No overlap
        let markdown = "Paragraph 1 that is longish.\n\nParagraph 2 that is also long.";
        let chunks = MarkdownChunker::chunk(markdown, &options);
        assert_eq!(chunks[0].content, "Paragraph 1 that is longish.");
        // Next chunk should NOT contain "longish" if overlap was skipped.
        assert!(!chunks[1].content.contains("longish"));

        // Single newline split -> Overlap
        let markdown = "Line 1 is quite long and stuff.\nLine 2 is here.";
        let chunks = MarkdownChunker::chunk(markdown, &options);
        // Overlap should be "and stuff." (10 chars roughly)
        assert!(chunks[1].content.contains("and stuff."));
    }
    #[test]
    fn test_char_boundary_panic() {
        // 'å' is 2 bytes: C3 A5
        // Generate a string where 'å' spans the max_size boundary
        let mut text = "a".repeat(999);
        text.push('å'); // bytes 999 and 1000
        text.push_str(" extra content");
        
        let options = ChunkerOptions {
            max_size: 1000,
            min_size: 10,
            overlap: 0,
        };
        
        // This should not panic
        let chunks = MarkdownChunker::chunk(&text, &options);
        assert!(!chunks.is_empty());
    }
}
