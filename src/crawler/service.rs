use anyhow::{Context, Result};
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::handler::viewport::Viewport;
use chromiumoxide::Page;
use futures::StreamExt;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

use super::event::CrawlerEvent;
use crate::app_event::AppEvent;
use crate::bus::EventBus;
use crate::event::Event;
use crate::parser::{Chunk, ChunkerOptions, Extractor, MarkdownChunker};

/// Configuration for the CrawlerService.
#[derive(Clone)]
pub struct CrawlerConfig {
    /// Number of concurrent crawl tasks (runners).
    pub concurrency: usize,
    /// Whether to run the browser in headless mode.
    pub headless: bool,
    /// The default user data directory. If None, a temporary one will be created.
    pub user_data_dir: Option<std::path::PathBuf>,
    /// Delegate used to decide whether a URL should be skipped.
    pub should_exclude_url: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    /// Maximum number of chunks emitted per crawled page.
    pub max_chunks: usize,
}

impl fmt::Debug for CrawlerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CrawlerConfig")
            .field("concurrency", &self.concurrency)
            .field("headless", &self.headless)
            .field("user_data_dir", &self.user_data_dir)
            .field("should_exclude_url", &"<delegate>")
            .field("max_chunks", &self.max_chunks)
            .finish()
    }
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        Self {
            concurrency: 2,
            headless: true,
            user_data_dir: None,
            should_exclude_url: Arc::new(|url: &str| {
                url == "https://doc.rust-lang.org/releases.html"
            }),
            max_chunks: 50,
        }
    }
}

/// A service that performs web crawling using a headless browser.
pub struct CrawlerService {
    bus: Arc<EventBus<AppEvent>>,
    config: CrawlerConfig,
}

impl CrawlerService {
    /// Creates a new `CrawlerService` with the specified configuration.
    pub fn new(bus: Arc<EventBus<AppEvent>>, config: CrawlerConfig) -> Self {
        Self { bus, config }
    }

    /// Starts the crawler service loop.
    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting CrawlerService (concurrency: {}, headless: {})...",
            self.config.concurrency, self.config.headless
        );

        // Prepare User Data Directory to avoid ProcessSingleton errors
        let _temp_dir;
        let mut builder = BrowserConfig::builder()
            .window_size(1920, 1080)
            .viewport(Viewport {
                width: 1920,
                height: 1080,
                ..Default::default()
            });

        if !self.config.headless {
            builder = builder.with_head();
        }

        if let Some(ref path) = self.config.user_data_dir {
            builder = builder.user_data_dir(path);
        } else {
            // Create a unique temporary directory for this browser instance
            _temp_dir = tempfile::tempdir()?;
            let temp_path = _temp_dir.path().to_path_buf();
            info!("Generated unique user data dir: {:?}", temp_path);
            builder = builder.user_data_dir(temp_path);
        }

        // Find chrome/chromium binary
        if let Ok(bin) = std::env::var("CHROME_BIN") {
             if std::path::Path::new(&bin).exists() {
                 info!("Using browser binary from CHROME_BIN: {}", bin);
                 builder = builder.chrome_executable(bin);
             }
        } else if cfg!(target_os = "macos") {
            let paths = [
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
                "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
                "/Applications/Chromium.app/Contents/MacOS/Chromium",
            ];
            for path in paths {
                if std::path::Path::new(path).exists() {
                    info!("Found browser at: {}", path);
                    builder = builder.chrome_executable(path);
                    break;
                }
            }
        }

        let (browser, mut handler) = Browser::launch(
            builder
                .build()
                .map_err(|e| anyhow::anyhow!("Failed to build browser config: {}", e))?,
        )
        .await?;

        // Wrap the browser in an Arc for shared access
        let browser = Arc::new(browser);

        // Spawn browser handler
        let mut handler_handle = tokio::spawn(async move {
            while let Some(h) = handler.next().await {
                if let Err(e) = h {
                    error!("Browser handler error: {:?}", e);
                    break;
                }
            }
        });

        // --- Internal Task Queue ---
        let (tx, rx_internal) = mpsc::channel::<Event<AppEvent>>(100);
        let shared_rx = Arc::new(tokio::sync::Mutex::new(rx_internal));

        // Spawn Runner Workers
        for i in 0..self.config.concurrency {
            let browser_clone = Arc::clone(&browser);
            let bus_clone = Arc::clone(&self.bus);
            let rx_worker = Arc::clone(&shared_rx);
            let should_exclude_url = Arc::clone(&self.config.should_exclude_url);
            let max_chunks = self.config.max_chunks;

            tokio::spawn(async move {
                info!("Runner #{} started", i);

                // Initialize a single page for this runner to reuse
                let page = match browser_clone.new_page("about:blank").await {
                    Ok(p) => p,
                    Err(e) => {
                        error!("Runner #{} failed to create initial page: {:?}", i, e);
                        return;
                    }
                };

                loop {
                    let event = {
                        let mut lock = rx_worker.lock().await;
                        lock.recv().await
                    };

                    if let Some(event) = event {
                        if let AppEvent::Crawler(CrawlerEvent::CrawlRequested {
                            url,
                            wait_selector,
                        }) = &event.payload
                        {
                            let url = url.clone();
                            let wait_selector = wait_selector.clone();

                            if should_exclude_url(&url) {
                                info!("Runner #{} skipping excluded URL: {}", i, url);
                                continue;
                            }

                            info!("Runner #{} processing: {}", i, url);
                            match Self::process_crawl(
                                &page,
                                &url,
                                wait_selector,
                                max_chunks,
                            )
                            .await
                            {
                                Ok((title, links, chunks)) => {
                                    info!(
                                        "Runner #{} crawled: {} ({} links, {} chunks)",
                                        i,
                                        url,
                                        links.len(),
                                        chunks.len()
                                    );
                                    let result_event = Event::new(
                                        event.aggregate_id.clone(),
                                        event.sequence_num + 1,
                                        AppEvent::Crawler(CrawlerEvent::PageIngested {
                                            url,
                                            title,
                                            links,
                                            chunks,
                                        }),
                                    );
                                    let _ = bus_clone.publish(result_event);
                                }
                                Err(e) => {
                                    error!("Runner #{} failed to crawl {}: {:?}", i, url, e);
                                    let error_event = Event::new(
                                        event.aggregate_id.clone(),
                                        event.sequence_num + 1,
                                        AppEvent::Crawler(CrawlerEvent::CrawlFailed {
                                            url,
                                            error: e.to_string(),
                                        }),
                                    );
                                    let _ = bus_clone.publish(error_event);
                                }
                            }
                        }
                    } else {
                        break; // Channel closed
                    }
                }
                info!("Runner #{} stopping", i);
                let _ = page.close().await;
            });
        }

        // --- Main Event Loop ---
        let mut rx = self.bus.subscribe();
        loop {
            tokio::select! {
                Ok(event) = rx.recv() => {
                    if let AppEvent::Crawler(CrawlerEvent::CrawlRequested { .. }) = &event.payload {
                        // Queue the task
                        if let Err(e) = tx.send(event).await {
                            error!("Failed to queue crawl task: {:?}", e);
                        }
                    }
                }
                _ = &mut handler_handle => {
                    error!("Browser handler task exited unexpectedly.");
                    break;
                }
                else => break,
            }
        }

        Ok(())
    }

    /// Fetches the page, immediately converts the HTML to links + chunks in a blocking thread,
    /// and drops the raw HTML before returning. This ensures multi-megabyte Chrome DOM strings
    /// never enter the event-bus ring buffer.
    async fn process_crawl(
        page: &Page,
        url: &str,
        wait_selector: Option<String>,
        max_chunks: usize,
    ) -> Result<(String, Vec<String>, Vec<Chunk>)> {
        page.goto(url)
            .await
            .context("Failed to navigate to URL")?;

        if let Some(selector) = wait_selector {
            info!("Waiting for selector: {}", selector);
            page.find_element(&selector)
                .await
                .context(format!("Failed to find selector: {}", selector))?;
        } else {
            // Navigation to non-HTML (like PDFs) sometimes doesn't trigger 'load' event.
            // We use a short timeout to handle cases where Chrome's PDF viewer is slow to settle.
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                page.wait_for_navigation()
            ).await;
        }

        let content_type = page
            .evaluate("document.contentType")
            .await?
            .into_value::<String>()
            .unwrap_or_default();

        let is_pdf = url.to_lowercase().ends_with(".pdf") || content_type == "application/pdf";

        let title = page
            .evaluate("document.title")
            .await?
            .into_value::<String>()
            .unwrap_or_else(|_| url.split('/').last().unwrap_or("PDF Document").to_string());

        if is_pdf {
            info!("PDF detected: {}. Extracting text...", url);
            // Fetch bytes via JS to reuse any browser-level session/cookies.
            // We use a blob → arrayBuffer → base64 conversion.
            let b64_script = r#"
                (async () => {
                    const resp = await fetch(window.location.href);
                    const buf = await resp.arrayBuffer();
                    const bytes = new Uint8Array(buf);
                    let binary = '';
                    for (let i = 0; i < bytes.byteLength; i++) {
                        binary += String.fromCharCode(bytes[i]);
                    }
                    return btoa(binary);
                })()
            "#;
            
            let b64: String = page.evaluate(b64_script).await?
                .into_value::<String>()
                .context("Failed to fetch PDF bytes via JS")?;
            
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(b64.replace(['\n', '\r'], ""))
                .context("Failed to decode base64 PDF data")?;

            let (links, chunks) = tokio::task::spawn_blocking(move || -> Result<(Vec<String>, Vec<Chunk>)> {
                let text = Extractor::extract_pdf_text(&bytes)?;
                let mut chunks = MarkdownChunker::chunk(
                    &text,
                    &ChunkerOptions {
                        max_size: 1000,
                        ..Default::default()
                    },
                );
                if chunks.len() > max_chunks {
                    chunks.truncate(max_chunks);
                }
                // PDF link extraction could be added here later if needed
                Ok((Vec::new(), chunks))
            }).await.context("Blocking PDF extraction task panicked")??;

            return Ok((title, links, chunks));
        }

        let content = page.content().await.context("Failed to get page content")?;
        
        // Convert HTML → links + chunks in a blocking thread.
        // `content` is moved into the closure and freed after chunking — it never enters the bus.
        let url_owned = url.to_string();
        let (links, chunks) =
            tokio::task::spawn_blocking(move || -> Result<(Vec<String>, Vec<Chunk>)> {
                let html = scraper::Html::parse_document(&content);
                let links = Extractor::extract_links(&html, &url_owned);
                drop(html); // free DOM #1 before markdown conversion

                let markdown = Extractor::to_markdown(&content, &url_owned, &[]);
                drop(content); // free the raw HTML string — it's no longer needed

                let mut chunks = MarkdownChunker::chunk(
                    &markdown,
                    &ChunkerOptions {
                        max_size: 1000,
                        ..Default::default()
                    },
                );
                if chunks.len() > max_chunks {
                    chunks.truncate(max_chunks);
                }
                Ok((links, chunks))
            })
            .await
            .context("Blocking extraction task panicked")??;

        Ok((title, links, chunks))
    }
}
