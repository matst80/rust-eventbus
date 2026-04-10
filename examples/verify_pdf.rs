use anyhow::Result;
use rust_eventbus::crawler::{CrawlerConfig, CrawlerService};
use rust_eventbus::bus::EventBus;
use rust_eventbus::app_event::AppEvent;
use rust_eventbus::crawler::event::CrawlerEvent;
use rust_eventbus::event::Event;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let bus = Arc::new(EventBus::<AppEvent>::new(1024));
    let config = CrawlerConfig {
        concurrency: 1,
        headless: true, // PDF extraction works in headless too
        ..Default::default()
    };

    let crawler = CrawlerService::new(Arc::clone(&bus), config);
    let crawler_handle = tokio::spawn(async move {
        if let Err(e) = crawler.run().await {
            eprintln!("Crawler error: {:?}", e);
        }
    });

    let mut rx = bus.subscribe();

    // Wait for crawler to initialize
    sleep(Duration::from_secs(2)).await;

    // Request PDF crawl
    let pdf_url = "https://pdfobject.com/pdf/sample.pdf";
    println!("Requesting PDF crawl: {}", pdf_url);
    
    let crawl_event = Event::new(
        "test-pdf".to_string(),
        0,
        AppEvent::Crawler(CrawlerEvent::CrawlRequested {
            url: pdf_url.to_string(),
            wait_selector: None,
            max_chunks: None,
        }),
    );
    bus.publish(crawl_event)?;

    // Wait for result
    let mut found = false;
    let timeout = sleep(Duration::from_secs(30));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        if let AppEvent::Crawler(CrawlerEvent::PageIngested { url, title, chunks, .. }) = event.payload {
                            if url == pdf_url {
                                println!("Successfully ingested PDF!");
                                println!("Title: {}", title);
                                println!("Chunks found: {}", chunks.len());
                                for (i, chunk) in chunks.iter().enumerate().take(3) {
                                    println!("Chunk #{}:\n{}", i, chunk.content);
                                }
                                found = true;
                                break;
                            }
                        } else if let AppEvent::Crawler(CrawlerEvent::CrawlFailed { url, error }) = event.payload {
                            if url == pdf_url {
                                eprintln!("Crawl failed for {}: {}", url, error);
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            _ = &mut timeout => {
                println!("Timed out waiting for PDF ingestion");
                break;
            }
        }
    }

    if !found {
        anyhow::bail!("Failed to ingest PDF");
    }

    Ok(())
}
