use scraper::{Html, Selector};
use url::Url;
use std::collections::HashMap;
use serde_json::Value;

pub struct Extractor;

impl Extractor {
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
        let ld_json_selector = Selector::parse("script[type=\"application/ld+json\"]").expect("Valid selector");
        let json_selector = Selector::parse("script[type=\"application/json\"]").expect("Valid selector");
        
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
        n.replace("og:", "").replace("twitter:", "").replace("itemprop:", "").replace("article:", "")
    }

    fn is_excluded_meta(element: &scraper::ElementRef) -> bool {
        let name = element.value().attr("name")
            .or_else(|| element.value().attr("property"))
            .or_else(|| element.value().attr("itemprop"));
        
        match name {
            Some(n) => {
                let n = Self::normalize_meta_name(n);
                ["robots", "viewport", "theme-color", "image", "type", "card", "label1", "data1", "site_name", "url", "msapplication-tileimage", "locale"].contains(&n.as_str())
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
            let name = element.value().attr("name")
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
            "nav", "footer", "header", "aside", 
            "script", "style", "svg", "noscript", "iframe",
            ".ads", ".sidebar", ".menu", ".nav-container", ".footer-container",
            "#coiOverlay", ".ex-vat" // Specifically requested for testing
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

        // Conversion to markdown
        let raw_markdown = html2md::parse_html(&processed_html);
        
        // Post-process the markdown
        Self::clean_markdown(&raw_markdown, base_url)
    }

    /// Strips most images, keeping only the first eager-loaded one as a hero candidate.
    fn simplify_images(html: &str) -> String {
        let Ok(re_img) = regex::Regex::new(r"(?is)<img[^>]*>") else { return html.to_string(); };
        let mut first_eager_found = false;
        
        re_img.replace_all(html, |caps: &regex::Captures| {
            let tag = &caps[0];
            if !first_eager_found && tag.contains("loading=\"eager\"") {
                first_eager_found = true;
                let mut clean = String::from("<img");
                if let Some(src) = regex::Regex::new(r#"(?i)src="([^"]+)""#).ok().and_then(|r| r.captures(tag)).and_then(|c| c.get(1)) {
                    clean.push_str(&format!(" src=\"{}\"", src.as_str()));
                }
                if let Some(alt) = regex::Regex::new(r#"(?i)alt="([^"]+)""#).ok().and_then(|r| r.captures(tag)).and_then(|c| c.get(1)) {
                    clean.push_str(&format!(" alt=\"{}\"", alt.as_str()));
                }
                clean.push_str(">");
                clean
            } else {
                String::new()
            }
        }).to_string()
    }

    /// Cleans up generated markdown artifacts like empty links and fixes header formats.
    fn clean_markdown(markdown: &str, base_url: &str) -> String {
        let base = Url::parse(base_url).ok();
        
        // 1. Fix link whitespace and resolve relative URLs
        // Regex matches [ text ](url) including multi-line text
        let Ok(re_link) = regex::Regex::new(r"\[\s*([\s\S]*?)\s*\]\((.*?)\)") else { return markdown.to_string(); };
        let markdown = re_link.replace_all(markdown, |caps: &regex::Captures| {
            let text = caps[1].trim();
            // Simplify internal whitespace
            let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
            
            let mut href = caps[2].trim().to_string();
            if let Some(base) = &base {
                // Check if it's relative
                if !href.is_empty() && !href.contains("://") && !href.starts_with("mailto:") && !href.starts_with("tel:") && !href.starts_with('#') {
                    if let Ok(abs_url) = base.join(&href) {
                        href = abs_url.to_string();
                    }
                }
            }
            
            if text.is_empty() {
                // Remove links with no text content
                String::new()
            } else {
                format!("[{}]({})", text, href)
            }
        });

        let mut final_lines: Vec<String> = Vec::new();
        let raw_lines: Vec<&str> = markdown.lines().collect();
        let mut i = 0;
        
        while i < raw_lines.len() {
            let line = raw_lines[i];
            let trimmed = line.trim();
            
            // Skip empty links, empty list items, and empty headers
            let is_empty_header = trimmed.starts_with('#') && trimmed.chars().all(|c| c == '#' || c.is_whitespace());
            if trimmed == "[]()" || trimmed == "*" || trimmed == "-" || trimmed == "+" || is_empty_header { 
                i += 1; 
                continue; 
            }

            // Skip empty lines if the previous line was also empty
            if trimmed.is_empty() && !final_lines.is_empty() && final_lines.last().unwrap().trim().is_empty() {
                i += 1;
                continue;
            }

            if i + 1 < raw_lines.len() {
                let next_line = raw_lines[i+1].trim();
                // Setext to ATX conversion
                if !trimmed.is_empty() && (next_line.chars().all(|c| c == '=') && !next_line.is_empty()) {
                    if !final_lines.is_empty() && !final_lines.last().unwrap().trim().is_empty() { final_lines.push(String::new()); }
                    final_lines.push(format!("# {}", trimmed));
                    i += 2; continue;
                } else if !trimmed.is_empty() && (next_line.chars().all(|c| c == '-') && !next_line.is_empty()) {
                    if !final_lines.is_empty() && !final_lines.last().unwrap().trim().is_empty() { final_lines.push(String::new()); }
                    final_lines.push(format!("## {}", trimmed));
                    i += 2; continue;
                }
            }

            if trimmed.starts_with('#') {
                if !final_lines.is_empty() && !final_lines.last().unwrap().trim().is_empty() { final_lines.push(String::new()); }
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
}
