pub mod downloader;
pub mod event;
pub mod onnx;
pub mod processor;
pub mod projection;

pub use event::EmbeddingEvent;
pub use onnx::OnnxEmbeddingService;
pub use processor::EmbeddingProcessor;
pub use projection::{EmbeddingProjection, EmbeddingState};
