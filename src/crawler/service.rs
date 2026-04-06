use anyhow::{Context, Result};
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::handler::viewport::Viewport;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::bus::{EventBus};
use crate::event::{Event};
use super::event::CrawlerEvent;

/// Configuration for the CrawlerService.
#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    /// Number of concurrent crawl tasks (runners).
    pub concurrency: usize,
    /// Whether to run the browser in headless mode.
    pub headless: bool,
    /// The default user data directory. If None, a temporary one will be created.
    pub user_data_dir: Option<std::path::PathBuf>,
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        Self {
            concurrency: 2,
            headless: true,
            user_data_dir: None,
        }
    }
}

/// A service that performs web crawling using a headless browser.
pub struct CrawlerService {
    bus: Arc<EventBus<CrawlerEvent>>,
    config: CrawlerConfig,
}

impl CrawlerService {
    /// Creates a new `CrawlerService` with the specified configuration.
    pub fn new(bus: Arc<EventBus<CrawlerEvent>>, config: CrawlerConfig) -> Self {
        Self { bus, config }
    }

    /// Starts the crawler service loop.
    pub async fn run(&self) -> Result<()> {
        info!("Starting CrawlerService (concurrency: {}, headless: {})...", 
            self.config.concurrency, self.config.headless);

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

        // On Mac, try common Chrome/Chromium paths
        if cfg!(target_os = "macos") {
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
            builder.build()
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
        let (tx, rx_internal) = mpsc::channel::<Event<CrawlerEvent>>(100);
        let shared_rx = Arc::new(tokio::sync::Mutex::new(rx_internal));

        // Spawn Runner Workers
        for i in 0..self.config.concurrency {
            let browser_clone = Arc::clone(&browser);
            let bus_clone = Arc::clone(&self.bus);
            let rx_worker = Arc::clone(&shared_rx);

            tokio::spawn(async move {
                info!("Runner #{} started", i);
                loop {
                    let event = {
                        let mut lock = rx_worker.lock().await;
                        lock.recv().await
                    };

                    if let Some(event) = event {
                        if let CrawlerEvent::CrawlRequested { url, wait_selector } = event.payload {
                            info!("Runner #{} processing: {}", i, url);
                            match Self::process_crawl(browser_clone.clone(), &url, wait_selector).await {
                                Ok((content, title)) => {
                                    info!("Runner #{} successfully crawled: {}", i, url);
                                    let result_event = Event::new(
                                        event.aggregate_id.clone(),
                                        event.sequence_num + 1,
                                        CrawlerEvent::PageIngested { url, content, title },
                                    );
                                    let _ = bus_clone.publish(result_event);
                                }
                                Err(e) => {
                                    error!("Runner #{} failed to crawl {}: {:?}", i, url, e);
                                    let error_event = Event::new(
                                        event.aggregate_id.clone(),
                                        event.sequence_num + 1,
                                        CrawlerEvent::CrawlFailed { url, error: e.to_string() },
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
            });
        }

        // --- Main Event Loop ---
        let mut rx = self.bus.subscribe();
        loop {
            tokio::select! {
                Ok(event) = rx.recv() => {
                    if let CrawlerEvent::CrawlRequested { .. } = &event.payload {
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

    async fn process_crawl(browser: Arc<Browser>, url: &str, wait_selector: Option<String>) -> Result<(String, String)> {
        let page = browser.new_page(url).await.context("Failed to open new page")?;

        // Handle SPAs: if a selector is provided, wait for it.
        if let Some(selector) = wait_selector {
            info!("Waiting for selector: {}", selector);
            page.find_element(&selector)
                .await
                .context(format!("Failed to find selector: {}", selector))?;
        } else {
            page.wait_for_navigation().await.context("Wait for navigation failed")?;
        }

        let content = page.content().await.context("Failed to get page content")?;
        let title = page.evaluate("document.title")
            .await?
            .into_value::<String>()
            .context("Failed to get page title")?;

        // CRITICAL: Close page to free memory
        page.close().await.context("Failed to close page")?;

        Ok((content, title))
    }
}
