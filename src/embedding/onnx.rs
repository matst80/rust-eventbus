use std::path::Path;
use anyhow::Result;
use ndarray::{Array2, Axis};
use ort::{
    session::{builder::GraphOptimizationLevel, Session},
    value::Value,
    execution_providers::{CUDAExecutionProvider, CPUExecutionProvider, TensorRTExecutionProvider},
};
use parking_lot::Mutex;
use tokenizers::Tokenizer;

pub struct OnnxEmbeddingService {
    session: Mutex<Session>,
    tokenizer: Tokenizer,
}

impl OnnxEmbeddingService {
    pub fn new<P: AsRef<Path>>(model_path: P, tokenizer_path: P) -> Result<Self> {
        let tokenizer = Tokenizer::from_file(tokenizer_path.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to load tokenizer: {}", e))?;

        let mut builder = Session::builder()
            .map_err(|e| anyhow::anyhow!("Failed to create builder: {:?}", e))?;
        
        builder = builder.with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(|e| anyhow::anyhow!("Failed to set optimization: {:?}", e))?;

        // Try GPU (TensorRT then CUDA)
        let session = match builder.clone().with_execution_providers([
            TensorRTExecutionProvider::default().build(),
            CUDAExecutionProvider::default().build(),
        ]) {
            Ok(mut b) => match b.commit_from_file(&model_path) {
                Ok(s) => {
                    println!("Successfully initialized ONNX with GPU (TensorRT/CUDA)");
                    s
                },
                Err(e) => {
                    eprintln!("Failed to commit GPU session: {:?}. Falling back to CPU.", e);
                    builder.with_execution_providers([CPUExecutionProvider::default().build()])
                        .map_err(|e| anyhow::anyhow!("Failed to set CPU provider: {:?}", e))?
                        .commit_from_file(model_path)
                        .map_err(|e| anyhow::anyhow!("Failed to load model on CPU: {:?}", e))?
                }
            },
            Err(e) => {
                eprintln!("Failed to set GPU execution providers: {:?}. Falling back to CPU.", e);
                builder.with_execution_providers([CPUExecutionProvider::default().build()])
                    .map_err(|e| anyhow::anyhow!("Failed to set CPU provider: {:?}", e))?
                    .commit_from_file(model_path)
                    .map_err(|e| anyhow::anyhow!("Failed to load model on CPU: {:?}", e))?
            }
        };

        Ok(Self { session: Mutex::new(session), tokenizer })
    }

    pub fn session(&self) -> &Mutex<Session> {
        &self.session
    }

    pub fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let results = self.embed_batch(&vec![text.to_string()])?;
        Ok(results.into_iter().next().unwrap())
    }

    pub fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        let encodings = self.tokenizer.encode_batch(texts.to_vec(), true)
            .map_err(|e| anyhow::anyhow!("Tokenization failed: {}", e))?;

        let batch_size = texts.len();
        let seq_len = encodings[0].len();
        
        // Prepare input tensors
        let mut input_ids = Vec::with_capacity(batch_size * seq_len);
        let mut attention_mask = Vec::with_capacity(batch_size * seq_len);
        let mut token_type_ids = Vec::with_capacity(batch_size * seq_len);

        for enc in &encodings {
            input_ids.extend(enc.get_ids().iter().map(|&x| x as i64));
            attention_mask.extend(enc.get_attention_mask().iter().map(|&x| x as i64));
            token_type_ids.extend(enc.get_type_ids().iter().map(|&x| x as i64));
        }

        let input_ids_array = Array2::from_shape_vec((batch_size, seq_len), input_ids)?;
        let attention_mask_array = Array2::from_shape_vec((batch_size, seq_len), attention_mask)?;
        let token_type_ids_array = Array2::from_shape_vec((batch_size, seq_len), token_type_ids)?;

        let results = {
            let mut session = self.session.lock();
            let outputs = session.run(ort::inputs![
                "input_ids" => Value::from_array(input_ids_array).map_err(|e| anyhow::anyhow!("{:?}", e))?,
                "attention_mask" => Value::from_array(attention_mask_array).map_err(|e| anyhow::anyhow!("{:?}", e))?,
                "token_type_ids" => Value::from_array(token_type_ids_array).map_err(|e| anyhow::anyhow!("{:?}", e))?,
            ]).map_err(|e| anyhow::anyhow!("Inference failed: {:?}", e))?;

            // Output shape is [batch_size, seq_len, 384 for bge-small]
            let hidden_states = outputs[0].try_extract_array::<f32>().map_err(|e| anyhow::anyhow!("{:?}", e))?;
            
            let mut batch_results = Vec::with_capacity(batch_size);

            for i in 0..batch_size {
                // CLS token is at index 0 of seq_len
                let cls_embedding: Vec<f32> = hidden_states
                    .index_axis(Axis(0), i) // Get batch i
                    .index_axis(Axis(0), 0) // Get CLS token
                    .iter()
                    .copied()
                    .collect();

                // L2 Normalization
                let mut sum_sq: f32 = 0.0;
                for &x in &cls_embedding {
                    sum_sq += x * x;
                }
                let norm = sum_sq.sqrt().max(1e-12);
                batch_results.push(cls_embedding.into_iter().map(|x| x / norm).collect());
            }
            batch_results
        };

        Ok(results)
    }
}
