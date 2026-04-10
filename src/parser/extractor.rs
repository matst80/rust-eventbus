use lopdf::Document;
use scraper::{Html, Selector};
use serde_json::Value;
use std::collections::HashMap;
use url::Url;
use anyhow::{Result, Context};

pub struct Extractor;

impl Extractor {
    /// Extracts text from a PDF document.
    pub fn extract_pdf_text(data: &[u8]) -> Result<String> {
        let doc = Document::load_mem(data).context("Failed to load PDF from memory")?;
        let pages = doc.get_pages();
        let mut page_numbers: Vec<u32> = pages.keys().cloned().collect();
        page_numbers.sort_unstable();
        
        let text = doc.extract_text(&page_numbers).context("Failed to extract text from PDF")?;
        Ok(text.replace('\0', ""))
    }

    /// Extracts absolute links from the HTML content.
    pub fn extract_links(html: &Html, base_url: &str) -> Vec<String> {
        let Ok(base) = Url::parse(base_url) else {
            return Vec::new();
        };

        let selector = Selector::parse("a[href]").expect("Valid selector");
        let mut links = Vec::new();

        for element in html.select(&selector) {
            if let Some(href) = element.value().attr("href") {
                if let Ok(url) = base.join(href) {
                    // Only keep http/https links
                    if url.scheme() == "http" || url.scheme() == "https" {
                        links.push(url.to_string());
                    }
                }
            }
        }

        links.sort();
        links.dedup();
        links
    }

    /// Extracts structured data (LD-JSON and other application/json scripts).
    pub fn extract_structured_data(html: &Html) -> Vec<Value> {
        let ld_json_selector =
            Selector::parse("script[type=\"application/ld+json\"]").expect("Valid selector");
        let json_selector =
            Selector::parse("script[type=\"application/json\"]").expect("Valid selector");

        let mut data = Vec::new();

        for selector in [ld_json_selector, json_selector] {
            for element in html.select(&selector) {
                let content = element.text().collect::<Vec<_>>().join("");
                if let Ok(json) = serde_json::from_str::<Value>(&content) {
                    data.push(json);
                }
            }
        }

        data
    }

    fn normalize_meta_name(name: &str) -> String {
        let n = name.to_lowercase();
        n.replace("og:", "")
            .replace("twitter:", "")
            .replace("itemprop:", "")
            .replace("article:", "")
    }

    fn is_excluded_meta(element: &scraper::ElementRef) -> bool {
        let name = element
            .value()
            .attr("name")
            .or_else(|| element.value().attr("property"))
            .or_else(|| element.value().attr("itemprop"));

        match name {
            Some(n) => {
                let n = Self::normalize_meta_name(n);
                [
                    "robots",
                    "viewport",
                    "theme-color",
                    "image",
                    "type",
                    "card",
                    "label1",
                    "data1",
                    "site_name",
                    "url",
                    "msapplication-tileimage",
                    "locale",
                ]
                .contains(&n.as_str())
            }
            None => true,
        }
    }

    /// Extracts metadata (Meta tags, OpenGraph).
    /// If there are multiple values for the same key, keeps the longest one.
    pub fn extract_metadata(html: &Html) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        let selector = Selector::parse("meta").expect("Valid selector");

        for element in html.select(&selector) {
            if Self::is_excluded_meta(&element) {
                continue;
            }
            let name = element
                .value()
                .attr("name")
                .or_else(|| element.value().attr("property"))
                .or_else(|| element.value().attr("itemprop"));

            let content = element.value().attr("content");

            if let (Some(n), Some(c)) = (name, content) {
                let n = Self::normalize_meta_name(n);

                let existing = metadata.entry(n).or_insert_with(|| c.to_string());
                if c.len() > existing.len() {
                    *existing = c.to_string();
                }
            }
        }

        metadata
    }

    pub fn to_markdown(html_str: &str, base_url: &str, extra_excludes: &[&str]) -> String {
        let mut html = Html::parse_fragment(html_str);

        // Dynamic list of excludes for noise reduction
        let mut excludes = vec![
            "nav",
            "footer",
            "header",
            "aside",
            "script",
            "style",
            "svg",
            "noscript",
            "iframe",
            ".ads",
            ".sidebar",
            ".menu",
            ".nav-container",
            ".footer-container",
            "#coiOverlay",
            ".ex-vat", // Specifically requested for testing
        ];

        excludes.extend(extra_excludes.iter().copied());

        let mut to_remove = Vec::new();
        for sel_str in excludes {
            if let Ok(selector) = Selector::parse(sel_str) {
                for element in html.select(&selector) {
                    to_remove.push(element.id());
                }
            }
        }

        // Remove excluded elements from the tree
        for id in to_remove {
            if let Some(mut node) = html.tree.get_mut(id) {
                node.detach();
            }
        }

        let mut processed_html = html.root_element().html();

        // Image simplification
        processed_html = Self::simplify_images(&processed_html);

        // Resolve relative URLs in <a href> attributes so html2md emits absolute links.
        processed_html = Self::preprocess_links(&processed_html, base_url);

        // Normalize code blocks: strip spans/buttons inside <pre> so html2md
        // sees plain text and preserves newlines correctly.
        processed_html = Self::preprocess_code_blocks(&processed_html);

        // Conversion to markdown
        let raw_markdown = html2md::parse_html(&processed_html);

        // Post-process the markdown
        let markdown = Self::clean_markdown(&raw_markdown, base_url);
        
        // Strip null bytes for DB safety
        markdown.replace('\0', "")
    }

    /// Strips most images, keeping only the first eager-loaded one as a hero candidate.
    fn simplify_images(html: &str) -> String {
        let Ok(re_img) = regex::Regex::new(r"(?is)<img[^>]*>") else {
            return html.to_string();
        };
        let mut first_eager_found = false;

        re_img
            .replace_all(html, |caps: &regex::Captures| {
                let tag = &caps[0];
                if !first_eager_found && tag.contains("loading=\"eager\"") {
                    first_eager_found = true;
                    let mut clean = String::from("<img");
                    if let Some(src) = regex::Regex::new(r#"(?i)src="([^"]+)""#)
                        .ok()
                        .and_then(|r| r.captures(tag))
                        .and_then(|c| c.get(1))
                    {
                        clean.push_str(&format!(" src=\"{}\"", src.as_str()));
                    }
                    if let Some(alt) = regex::Regex::new(r#"(?i)alt="([^"]+)""#)
                        .ok()
                        .and_then(|r| r.captures(tag))
                        .and_then(|c| c.get(1))
                    {
                        clean.push_str(&format!(" alt=\"{}\"", alt.as_str()));
                    }
                    clean.push_str(">");
                    clean
                } else {
                    String::new()
                }
            })
            .to_string()
    }

    /// Resolves relative `href` attributes to absolute URLs and removes anchor
    /// elements that carry no text (image-only or whitespace-only links), so
    /// html2md receives clean, absolute links without any post-processing.
    fn preprocess_links(html: &str, base_url: &str) -> String {
        let Ok(base) = Url::parse(base_url) else {
            return html.to_string();
        };
        let resolve = |attr: &str, val: &str| -> String {
            if val.is_empty()
                || val.contains("://")
                || val.starts_with("mailto:")
                || val.starts_with("tel:")
                || val.starts_with('#')
                || val.starts_with("data:")
            {
                format!("{}=\"{}\"", attr, val)
            } else if let Ok(abs) = base.join(val) {
                format!("{}=\"{}\"", attr, abs)
            } else {
                format!("{}=\"{}\"", attr, val)
            }
        };

        // 1. Resolve relative hrefs and srcs.
        let Ok(re_href) = regex::Regex::new(r#"(?i)\b(href|src)="([^"]*)""#) else {
            return html.to_string();
        };
        let html = re_href.replace_all(html, |caps: &regex::Captures| {
            resolve(&caps[1], &caps[2])
        });
        // 2. Remove <a> tags that have no visible text content (e.g. image-only links).
        let Ok(re_empty_a) = regex::Regex::new(r"(?is)<a\b[^>]*>\s*</a>") else {
            return html.into_owned();
        };
        re_empty_a.replace_all(&html, "").into_owned()
    }

    /// Strips all HTML tags inside `<pre>` blocks so that html2md sees plain text
    /// and preserves newlines correctly (syntax-highlight spans and UI buttons
    /// inside `<pre>` would otherwise cause html2md to collapse code onto one line).
    fn preprocess_code_blocks(html: &str) -> String {
        let Ok(re) = regex::Regex::new(r"(?is)<pre[^>]*>(.*?)</pre>") else {
            return html.to_string();
        };
        let Ok(re_tag) = regex::Regex::new(r"<[^>]+>") else {
            return html.to_string();
        };

        re.replace_all(html, |caps: &regex::Captures| {
            let inner = &caps[1];
            // Strip all HTML tags, leaving raw text nodes and their newlines intact.
            let text = re_tag.replace_all(inner, "");
            // Decode the common HTML entities that appear in code examples.
            let text = text
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&#39;", "'")
                .replace("&nbsp;", " ");
            // Re-encode so the text is safe inside an HTML element.
            let encoded = text
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;");
            format!("<pre><code>{}</code></pre>", encoded)
        })
        .to_string()
    }

    /// Cleans up generated markdown artifacts like empty links and fixes header formats.
    /// URL resolution is handled upstream in `preprocess_links` (HTML stage), so this
    /// function only needs a simple single-line regex — no risk of matching across
    /// code-block boundaries.
    fn clean_markdown(markdown: &str, _base_url: &str) -> String {
        // Collapse internal whitespace in link text and drop empty links.
        // Single-line match ([^\]\n] / [^\)\n]) is safe by construction: patterns like
        // `error[E0499]` inside code blocks never have `](url)` on the same line.
        let Ok(re_link) = regex::Regex::new(r"\[([^\]\n]*)\]\(([^\)\n]*)\)") else {
            return markdown.to_string();
        };
        let markdown = re_link.replace_all(markdown, |caps: &regex::Captures| {
            let text = caps[1].split_whitespace().collect::<Vec<_>>().join(" ");
            if text.is_empty() {
                String::new()
            } else {
                format!("[{}]({})", text, caps[2].trim())
            }
        });
        let markdown = markdown.as_ref();

        let mut final_lines: Vec<String> = Vec::new();
        let raw_lines: Vec<&str> = markdown.lines().collect();
        let mut i = 0;
        let mut in_fenced_block = false;

        while i < raw_lines.len() {
            let line = raw_lines[i];
            let trimmed = line.trim();

            if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
                if !in_fenced_block
                    && !final_lines.is_empty()
                    && !final_lines.last().unwrap().trim().is_empty()
                {
                    final_lines.push(String::new());
                }
                final_lines.push(line.to_string());
                in_fenced_block = !in_fenced_block;
                i += 1;
                continue;
            }

            if in_fenced_block {
                final_lines.push(line.to_string());
                i += 1;
                continue;
            }

            // Skip empty links, empty list items, and empty headers
            let is_empty_header =
                trimmed.starts_with('#') && trimmed.chars().all(|c| c == '#' || c.is_whitespace());
            if trimmed == "[]()"
                || trimmed == "*"
                || trimmed == "-"
                || trimmed == "+"
                || is_empty_header
            {
                i += 1;
                continue;
            }

            // Preserve paragraph breaks, but collapse runs of 3+ blank lines to a single blank line
            if trimmed.is_empty() {
                let prev_is_empty = final_lines
                    .last()
                    .map(|l| l.trim().is_empty())
                    .unwrap_or(false);
                let next_is_empty = i + 1 < raw_lines.len() && raw_lines[i + 1].trim().is_empty();
                if prev_is_empty || next_is_empty {
                    i += 1;
                    continue;
                }
                final_lines.push(String::new());
                i += 1;
                continue;
            }

            if i + 1 < raw_lines.len() {
                let next_line = raw_lines[i + 1].trim();
                // Setext to ATX conversion
                if !trimmed.is_empty()
                    && (next_line.chars().all(|c| c == '=') && !next_line.is_empty())
                {
                    if !final_lines.is_empty() && !final_lines.last().unwrap().trim().is_empty() {
                        final_lines.push(String::new());
                    }
                    final_lines.push(format!("# {}", trimmed));
                    i += 2;
                    continue;
                } else if !trimmed.is_empty()
                    && (next_line.chars().all(|c| c == '-') && !next_line.is_empty())
                {
                    if !final_lines.is_empty() && !final_lines.last().unwrap().trim().is_empty() {
                        final_lines.push(String::new());
                    }
                    final_lines.push(format!("## {}", trimmed));
                    i += 2;
                    continue;
                }
            }

            if trimmed.starts_with('#') {
                if !final_lines.is_empty() && !final_lines.last().unwrap().trim().is_empty() {
                    final_lines.push(String::new());
                }
            }

            final_lines.push(line.to_string());
            i += 1;
        }

        final_lines.join("\n").trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_deduplication() {
        let html_content = r#"
            <html>
                <head>
                    <meta name="description" content="Short desc">
                    <meta name="description" content="This is a much longer description that should be kept.">
                    <meta name="og:description" content="Vår kundtjänst finns här för att hjälpa dig! Titta gärna igenom vanliga frågor innan du kontaktar oss, för att se om vi redan svarat på din fråga.">

                    <meta property="og:title" content="Long title">
                    <meta property="title" content="Short">
                </head>
            </html>
        "#;
        let html = Html::parse_document(html_content);
        let meta = Extractor::extract_metadata(&html);

        assert_eq!(meta.get("description").unwrap(), "Vår kundtjänst finns här för att hjälpa dig! Titta gärna igenom vanliga frågor innan du kontaktar oss, för att se om vi redan svarat på din fråga.");
        assert_eq!(meta.get("title").unwrap(), "Long title");
    }

    #[test]
    fn test_link_extraction() {
        let html_content = r#"
            <html>
                <body>
                    <a href="/relative">Relative</a>
                    <a href="https://example.com/absolute">Absolute</a>
                    <a href="mailto:test@example.com">Mailto</a>
                </body>
            </html>
        "#;
        let html = Html::parse_document(html_content);
        let links = Extractor::extract_links(&html, "https://test.com/path");

        assert!(links.contains(&"https://test.com/relative".to_string()));
        assert!(links.contains(&"https://example.com/absolute".to_string()));
        assert_eq!(links.len(), 2);
    }

    #[test]
    fn test_to_markdown_excludes() {
        let html = r#"
            <html>
                <body>
                    <header><h1>Header</h1></header>
                    <nav>Menu</nav>
                    <main>
                        <article>
                            <p>Real content</p>
                            <aside>Sidebar junk</aside>
                        </article>
                    </main>
                    <footer>Footer stuff</footer>
                </body>
            </html>
        "#;
        let markdown = Extractor::to_markdown(html, "https://test.com", &[]);
        assert!(markdown.contains("Real content"));
        assert!(!markdown.contains("Header"));
        assert!(!markdown.contains("Menu"));
        assert!(!markdown.contains("Footer"));
        assert!(!markdown.contains("Sidebar junk"));
    }

    #[test]
    fn test_to_markdown_extra_excludes() {
        let html = r#"
            <div id="coiOverlay">Should be gone</div>
            <div class="ex-vat">Should be gone</div>
            <div class="user-noise">Special noise</div>
            <p>Important content</p>
        "#;
        let markdown = Extractor::to_markdown(html, "https://test.com", &[".user-noise"]);
        assert!(markdown.contains("Important content"));
        assert!(!markdown.contains("Should be gone"));
        assert!(!markdown.contains("Special noise"));
    }

    #[test]
    fn test_image_simplification() {
        let html = r#"
            <div>
                <img src="hero.jpg" loading="eager" alt="Hero">
                <img src="noise1.jpg">
                <img src="noise2.jpg" loading="lazy">
            </div>
        "#;
        let markdown = Extractor::to_markdown(html, "https://test.com", &[]);
        assert!(markdown.contains("![Hero](https://test.com/hero.jpg)"));
        assert!(!markdown.contains("noise1.jpg"));
        assert!(!markdown.contains("noise2.jpg"));
    }
    #[test]
    fn test_to_markdown_link_cleanup() {
        let html = r#"
            <a href="/relative">
                <span>  OUTLET  </span>
            </a>
            <a href="https://example.com/abs">
                Line 1
                Line 2
            </a>
            <a href="/empty">   </a>
            <a href="/image-only"><img src="stripped.jpg"></a>
        "#;
        let markdown = Extractor::to_markdown(html, "https://test.com/base", &[]);
        assert!(markdown.contains("[OUTLET](https://test.com/relative)"));
        assert!(markdown.contains("[Line 1 Line 2](https://example.com/abs)"));
        assert!(!markdown.contains("/empty"));
        assert!(!markdown.contains("/image-only"));
    }

    #[test]
    fn test_code_block_newlines_preserved() {
        // Simulates mdBook-style <pre><code> blocks: buttons div + syntax-highlight spans
        let html = r#"<pre><div class="buttons"><button>Copy</button></div><code class="language-console hljs shell"><span class="hljs-meta">$</span><span class="bash"> cargo run</span>
   Compiling ownership v0.1.0
error[E0499]: cannot borrow `s` as mutable more than once
  |
4 |     let r1 = &amp;mut s;
  |              ^^^^^^</code></pre>"#;
        let markdown = Extractor::to_markdown(html, "https://test.com", &[]);
        // All lines should be on separate lines inside a fenced block
        assert!(markdown.contains("$ cargo run"), "missing first line");
        assert!(
            markdown.contains("$ cargo run\n   Compiling"),
            "newline after first line lost"
        );
        assert!(
            markdown.contains("&mut s;"),
            "HTML entity not decoded in code block"
        );
    }

    #[test]
    fn test_to_markdown_preserves_fenced_blocks_and_paragraph_spacing() {
        let markdown = r#"Here’s the error:

```
$ cargo run
error[E0499]: cannot borrow `s` as mutable more than once at a time
```

This error says the code is invalid.

As always, we can use curly brackets:

```rust
fn main() {
    let mut s = String::from("hello");

    let r1 = &mut s;
}
```

Next paragraph."#;

        let cleaned = Extractor::clean_markdown(markdown, "https://test.com");

        assert!(cleaned.contains("Here’s the error:\n\n```"));
        assert!(cleaned.contains("```\n\nThis error says the code is invalid."));
        assert!(cleaned.contains("As always, we can use curly brackets:\n\n```rust"));
        assert!(cleaned.contains("let mut s = String::from(\"hello\");\n\n    let r1 = &mut s;"));
        assert!(cleaned.contains("}\n```\n\nNext paragraph."));
    }
}
