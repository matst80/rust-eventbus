use anyhow::Result;
use bubbletea_rs::{Model, Program, Msg, Cmd, window_size, WindowSizeMsg, event::KeyMsg, event::QuitMsg};
use termimad::MadSkin;
use bubbletea_widgets::list::{Item, ItemDelegate, Model as List};
use crossterm::event::KeyCode;
use lipgloss_extras::prelude::Style;
use rust_eventbus::{
    embedding::OnnxEmbeddingService,
    graph::{Edge, GraphState},
};
use std::sync::Arc;
use std::fmt::Display;
use std::collections::HashSet;

// --- Helper for Similarity ---
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 { 0.0 } else { dot / (norm_a * norm_b) }
}

#[derive(Debug, Clone, PartialEq)]
enum AppMode {
    Search,
    Results,
    Detail,
}

#[derive(Debug, Clone, PartialEq)]
enum DetailFocus {
    Content,
    Relations,
}

// --- List Items ---

#[derive(Debug, Clone)]
struct SearchResultItem {
    vector_score: f32,
    rerank_score: f32,
    id: String,
    title: String,
    section: String,
}

impl Item for SearchResultItem {
    fn filter_value(&self) -> String {
        format!("{} {}", self.title, self.section)
    }
}

impl Display for SearchResultItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[R:{:.3} V:{:.3}] {} - {}", self.rerank_score, self.vector_score, self.title, self.section)
    }
}

#[derive(Debug, Clone)]
struct RelationItem {
    id: String,
    title: String,
    relation: String,
}

impl Item for RelationItem {
    fn filter_value(&self) -> String {
        format!("{} {}", self.relation, self.title)
    }
}

impl Display for RelationItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] -> {}", self.relation, self.title)
    }
}

// --- Delegate ---

#[derive(Clone)]
struct SimpleDelegate<T> {
    highlight_style: Style,
    normal_style: Style,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Item + Display + Send + Sync + 'static> ItemDelegate<T> for SimpleDelegate<T> {
    fn render(&self, m: &List<T>, index: usize, item: &T) -> String {
        if index == m.cursor() {
            format!("> {}", self.highlight_style.render(&item.to_string()))
        } else {
            format!("  {}", self.normal_style.render(&item.to_string()))
        }
    }

    fn height(&self) -> usize { 1 }
    fn spacing(&self) -> usize { 0 }
    fn update(&self, _msg: &Msg, _m: &mut List<T>) -> Option<Cmd> { None }
    fn short_help(&self) -> Vec<bubbletea_widgets::key::Binding> { Vec::new() }
    fn full_help(&self) -> Vec<Vec<bubbletea_widgets::key::Binding>> { Vec::new() }
}

// --- Main Model ---

struct SearchModel {
    onnx_service: Arc<OnnxEmbeddingService>,
    state: Arc<GraphState>,
    
    mode: AppMode,
    query: String,
    
    results_list: List<SearchResultItem>,
    relations_list: List<RelationItem>,
    
    focus: DetailFocus,
    content_scroll: usize,
    
    current_node_id: Option<String>,
    history: Vec<String>,

    terminal_width: usize,
    terminal_height: usize,
    
    // UI Styles
    title_style: Style,
    meta_style: Style,
    border_style: Style,
    rendered_content: Vec<String>,
}

impl SearchModel {
    fn new(onnx_service: Arc<OnnxEmbeddingService>, state: Arc<GraphState>) -> Self {
        let (w, h) = crossterm::terminal::size().unwrap_or((80, 24));
        
        let highlight = Style::new().background("#3d3d3d").foreground("#ffed00").bold(true);
        let normal = Style::new().foreground("#cccccc");
        
        let results_delegate = SimpleDelegate {
            highlight_style: highlight.clone(),
            normal_style: normal.clone(),
            _phantom: std::marker::PhantomData,
        };
        
        let relations_delegate = SimpleDelegate {
            highlight_style: highlight.clone(),
            normal_style: normal.clone(),
            _phantom: std::marker::PhantomData,
        };

        Self {
            onnx_service,
            state,
            mode: AppMode::Search,
            query: String::new(),
            
            results_list: List::new(Vec::new(), results_delegate, w as usize, (h as usize).saturating_sub(7))
                .with_title("Search Results")
                .with_show_help(false)
                .with_show_pagination(false),
                
            relations_list: List::new(Vec::new(), relations_delegate, w as usize, (h as usize * 3 / 12).max(5))
                .with_title("Relations (Neighbors)")
                .with_show_help(false),
            
            focus: DetailFocus::Relations,
            content_scroll: 0,
                
            current_node_id: None,
            history: Vec::new(),
            
            terminal_width: w as usize,
            terminal_height: h as usize,
            
            title_style: Style::new().bold(true).foreground("#00e5ff"),
            meta_style: Style::new().foreground("#888888").italic(true),
            border_style: Style::new().foreground("#444444"),
            rendered_content: Vec::new(),
        }
    }

    fn re_render_content(&mut self) {
        let Some(id) = &self.current_node_id else {
            self.rendered_content = vec!["[No node selected]".to_string()];
            return;
        };
        
        let Some(node) = self.state.nodes.get(id) else {
            self.rendered_content = vec![format!("[Error: Node not found in state: {}]", id)];
            return;
        };

        let Some(content) = node.metadata.get("content") else {
            self.rendered_content = vec!["[No content metadata available]".to_string()];
            return;
        };

        if content.is_empty() {
            self.rendered_content = vec!["[Content is empty]".to_string()];
            return;
        }

        let mut skin = MadSkin::default();
        // Customize to match our TUI theme
        skin.paragraph.set_fg(termimad::gray(20)); // close to #cccccc
        skin.bold.set_fg(termimad::rgb(255, 237, 0)); // #ffed00
        skin.italic.set_fg(termimad::gray(14)); // #888888
        skin.headers[0].set_fg(termimad::rgb(0, 229, 255)); // #00e5ff
        skin.headers[1].set_fg(termimad::rgb(0, 229, 255));
        skin.code_block.set_bg(termimad::gray(4));
        skin.inline_code.set_bg(termimad::gray(4));
        
        // Ensure width is reasonable
        let width = self.terminal_width.max(20);
        let text = skin.text(content, Some(width));
        
        // Use format! to ensure full rendering via Display
        let rendered = format!("{}", text);
        
        if rendered.is_empty() {
            self.rendered_content = vec!["[Rendering produced empty output]".to_string()];
        } else {
            self.rendered_content = rendered.lines().map(|s| s.to_string()).collect();
        }
    }

    fn perform_search(&mut self) -> Result<()> {
        if self.query.is_empty() {
            self.results_list.set_items(Vec::new());
            return Ok(());
        }

        let query_emb = self.onnx_service.embed(&self.query)?;
        let mut initial_results = Vec::new();

        // 1. STAGE 1: Vector Retrieval
        for (id, node) in &self.state.nodes {
            if let Some(node_emb) = &node.embedding {
                let score = cosine_similarity(&query_emb, node_emb);
                if score > 0.3 {
                    let title = node.metadata.get("title").or(node.metadata.get("page_title")).cloned().unwrap_or(id.clone());
                    let section = node.metadata.get("section").cloned().unwrap_or_default();
                    let content = node.metadata.get("content").cloned().unwrap_or_default();
                    initial_results.push((score, id.clone(), title, section, content));
                }
            }
        }

        // Sort by vector score to get candidates
        initial_results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // 2. STAGE 2: Reranking (Top 20)
        let query_terms: Vec<String> = self.query
            .to_lowercase()
            .split_whitespace()
            .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let mut final_results = Vec::new();
        for (vector_score, id, title, section, content) in initial_results.into_iter().take(20) {
            let mut boost = 0.0;
            let content_lower = content.to_lowercase();
            for term in &query_terms {
                if content_lower.contains(term) {
                    boost += 0.1;
                }
            }
            
            final_results.push(SearchResultItem { 
                vector_score, 
                rerank_score: vector_score + boost, 
                id, 
                title, 
                section 
            });
        }

        // Sort by rerank score for final display
        final_results.sort_by(|a, b| b.rerank_score.partial_cmp(&a.rerank_score).unwrap_or(std::cmp::Ordering::Equal));
        
        self.results_list.set_items(final_results);
        Ok(())
    }

    fn relation_from_perspective(node_id: &str, edge: &Edge) -> Option<(String, String)> {
        if edge.from == node_id && edge.to != node_id {
            let relation = match edge.relation.as_str() {
                "part_of" => "parent",
                other => other,
            };
            return Some((edge.to.clone(), relation.to_string()));
        }

        if edge.to == node_id && edge.from != node_id {
            let relation = match edge.relation.as_str() {
                "next" => "prev",
                "prev" => "next",
                "part_of" => "child",
                "links_to" => "linked_from",
                other => other,
            };
            return Some((edge.from.clone(), relation.to_string()));
        }

        None
    }

    fn update_relations(&mut self, node_id: &str) {
        let mut items = Vec::new();
        let mut seen = HashSet::new();
        for edge in self.state.edges.values() {
            let Some((other_id, relation)) = Self::relation_from_perspective(node_id, edge) else {
                continue;
            };

            if !seen.insert((other_id.clone(), relation.clone())) {
                continue;
            }

            if let Some(node) = self.state.nodes.get(&other_id) {
                let title = node.metadata.get("title").or(node.metadata.get("page_title")).cloned().unwrap_or(node.id.clone());
                items.push(RelationItem { id: node.id.clone(), title, relation });
            }
        }
        items.sort_by(|a, b| {
            a.relation
                .cmp(&b.relation)
                .then_with(|| a.title.cmp(&b.title))
                .then_with(|| a.id.cmp(&b.id))
        });
        self.relations_list.set_items(items);
        self.content_scroll = 0;
    }

    fn first_outgoing_relation_target(&self, node_id: &str, relation: &str) -> Option<String> {
        let mut targets = self.state.edges.values()
            .filter(|edge| edge.from == node_id && edge.relation == relation)
            .map(|edge| edge.to.clone())
            .collect::<Vec<_>>();
        targets.sort();
        targets.dedup();
        targets.into_iter().next()
    }

    fn navigate_to(&mut self, next_id: String) {
        if let Some(current_id) = self.current_node_id.replace(next_id.clone()) {
            self.history.push(current_id);
        }
        self.update_relations(&next_id);
        self.re_render_content();
        self.content_scroll = 0;
    }

    fn detail_layout(&self) -> (usize, usize) {
        let h = self.terminal_height;
        let rel_h = self.relations_list.height();
        
        let mut fixed = 11; // Basic fixed lines
        if let Some(id) = &self.current_node_id {
            if let Some(node) = self.state.nodes.get(id) {
                if node.metadata.contains_key("section") {
                    fixed += 1;
                }
            }
        }
        
        let content_h = h.saturating_sub(rel_h).saturating_sub(fixed);
        (content_h, rel_h)
    }
}

// Static storage for data injection into Model::init
static ONNX_SERVICE: std::sync::OnceLock<Arc<OnnxEmbeddingService>> = std::sync::OnceLock::new();
static GRAPH_STATE: std::sync::OnceLock<Arc<GraphState>> = std::sync::OnceLock::new();

struct GlobalModel(SearchModel);

impl Model for GlobalModel {
    fn init() -> (Self, Option<Cmd>) {
        let onnx = ONNX_SERVICE.get().expect("ONNX not initialized").clone();
        let state = GRAPH_STATE.get().expect("State not initialized").clone();
        (GlobalModel(SearchModel::new(onnx, state)), Some(window_size()))
    }

    fn update(&mut self, msg: Msg) -> Option<Cmd> {
        if let Some(size_msg) = msg.downcast_ref::<WindowSizeMsg>() {
            self.0.terminal_width = size_msg.width as usize;
            self.0.terminal_height = size_msg.height as usize;
            
            let h = self.0.terminal_height;
            let w = self.0.terminal_width;
            
            self.0.results_list.set_size(w, h.saturating_sub(7));
            self.0.relations_list.set_size(w, (h * 3 / 12).max(5));
            self.0.re_render_content();
            return None;
        }

        match self.0.mode {
            AppMode::Search => {
                if let Some(key_msg) = msg.downcast_ref::<KeyMsg>() {
                    match key_msg.key {
                        KeyCode::Esc => return Some(Box::pin(async { Some(Box::new(QuitMsg) as Msg) })),
                        KeyCode::Enter => {
                            let _ = self.0.perform_search();
                            self.0.mode = AppMode::Results;
                            return None;
                        }
                        KeyCode::Backspace => { self.0.query.pop(); }
                        KeyCode::Char(c) => { self.0.query.push(c); }
                        _ => {}
                    }
                }
            }
            AppMode::Results => {
                if let Some(key_msg) = msg.downcast_ref::<KeyMsg>() {
                    match key_msg.key {
                        KeyCode::Esc => { self.0.mode = AppMode::Search; return None; }
                        KeyCode::Enter => {
                            if !self.0.results_list.is_filtering() {
                                if let Some(item) = self.0.results_list.selected_item() {
                                    let id = item.id.clone();
                                    self.0.current_node_id = Some(id.clone());
                                    self.0.update_relations(&id);
                                    self.0.re_render_content();
                                    self.0.mode = AppMode::Detail;
                                    self.0.focus = DetailFocus::Relations;
                                    return None;
                                }
                            }
                        }
                        _ => {}
                    }
                }
                return self.0.results_list.update(msg);
            }
            AppMode::Detail => {
                if let Some(key_msg) = msg.downcast_ref::<KeyMsg>() {
                    match key_msg.key {
                        KeyCode::Esc => { self.0.mode = AppMode::Results; return None; }
                        KeyCode::Tab => {
                            self.0.focus = match self.0.focus {
                                DetailFocus::Content => DetailFocus::Relations,
                                DetailFocus::Relations => DetailFocus::Content,
                            };
                            return None;
                        }
                        KeyCode::Up | KeyCode::Char('k') if self.0.focus == DetailFocus::Content => {
                            self.0.content_scroll = self.0.content_scroll.saturating_sub(1);
                            return None;
                        }
                        KeyCode::Down | KeyCode::Char('j') if self.0.focus == DetailFocus::Content => {
                            let (content_h, _) = self.0.detail_layout();
                            let max_scroll = self.0.rendered_content.len().saturating_sub(content_h);
                            self.0.content_scroll = (self.0.content_scroll + 1).min(max_scroll);
                            return None;
                        }
                        KeyCode::PageUp if self.0.focus == DetailFocus::Content => {
                            let (content_h, _) = self.0.detail_layout();
                            self.0.content_scroll = self.0.content_scroll.saturating_sub(content_h);
                            return None;
                        }
                        KeyCode::PageDown if self.0.focus == DetailFocus::Content => {
                            let (content_h, _) = self.0.detail_layout();
                            let max_scroll = self.0.rendered_content.len().saturating_sub(content_h);
                            self.0.content_scroll = (self.0.content_scroll + content_h).min(max_scroll);
                            return None;
                        }
                        KeyCode::Char('p') => {
                            if let Some(id) = &self.0.current_node_id {
                                let parent_id = self.0.first_outgoing_relation_target(id, "part_of");

                                if let Some(pid) = parent_id {
                                    self.0.navigate_to(pid);
                                    return None;
                                }
                            }
                        }
                        KeyCode::Char('.') => {
                            if let Some(id) = &self.0.current_node_id {
                                let next_id = self.0.first_outgoing_relation_target(id, "next");

                                if let Some(next_id) = next_id {
                                    self.0.navigate_to(next_id);
                                    return None;
                                }
                            }
                        }
                        KeyCode::Char(',') => {
                            if let Some(id) = &self.0.current_node_id {
                                let prev_id = self.0.first_outgoing_relation_target(id, "prev");

                                if let Some(prev_id) = prev_id {
                                    self.0.navigate_to(prev_id);
                                    return None;
                                }
                            }
                        }
                        KeyCode::Backspace => {
                            if !self.0.relations_list.is_filtering() {
                                if let Some(prev) = self.0.history.pop() {
                                    self.0.current_node_id = Some(prev.clone());
                                    self.0.update_relations(&prev);
                                } else {
                                    self.0.mode = AppMode::Results;
                                }
                                return None;
                            }
                        }
                        KeyCode::Enter => {
                             if !self.0.relations_list.is_filtering() && self.0.focus == DetailFocus::Relations {
                                 if let Some(item) = self.0.relations_list.selected_item() {
                                     let next_id = item.id.clone();
                                     if let Some(id) = self.0.current_node_id.take() {
                                         self.0.history.push(id);
                                     }
                                     self.0.current_node_id = Some(next_id.clone());
                                     self.0.update_relations(&next_id);
                                     return None;
                                 }
                             }
                        }
                        _ => {}
                    }
                }
                
                if self.0.focus == DetailFocus::Relations {
                    return self.0.relations_list.update(msg);
                }
            }
        }
        None
    }

    fn view(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!("{}\n", self.0.title_style.render("=== Knowledge Graph Navigator ===")));
        
        match self.0.mode {
            AppMode::Search => {
                s.push_str("\n🔍 Search nodes by content similarity:\n");
                s.push_str(&format!("> {}_\n", self.0.query));
                s.push_str("\n[Enter] Search | [Esc] Exit");
            }
            AppMode::Results => {
                s.push_str(&format!("\nResults for: '{}'\n", self.0.query));
                s.push_str(&self.0.results_list.view());
                s.push_str("\n[Esc] Back to Search");
            }
            AppMode::Detail => {
                if let Some(id) = &self.0.current_node_id {
                    let node = self.0.state.nodes.get(id).unwrap();
                    let title = node.metadata.get("title").or(node.metadata.get("page_title")).unwrap_or(id);
                    let section = node.metadata.get("section").cloned().unwrap_or_default();
                    
                    // --- Header ---
                    s.push_str(&format!("📍 Node: {}\n", self.0.title_style.render(title)));
                    if !section.is_empty() {
                        s.push_str(&format!("Section: {}\n", self.0.meta_style.render(&section)));
                    }
                    s.push_str(&format!("URL: {}\n", id));
                    
                    // --- Content Area ---
                    let (content_h, _) = self.0.detail_layout();
                    
                    let focus_c = if self.0.focus == DetailFocus::Content { " [SCROLLING]" } else { "" };
                    s.push_str(&format!("\n--- Content{} ---\n", focus_c));
                    
                    if !self.0.rendered_content.is_empty() {
                        let lines = &self.0.rendered_content;
                        let scroll = self.0.content_scroll.min(lines.len().saturating_sub(1));
                        let visible_lines: Vec<_> = lines.iter().skip(scroll).take(content_h).collect();
                        
                        s.push_str(&format!(" [Scroll: {}/{} | Lines: {}]\n", scroll, lines.len().saturating_sub(content_h), lines.len()));
                        for line in visible_lines {
                            s.push_str(&format!("{}\n", line));
                        }
                        
                        // Fill remaining content_h if content is short
                        for _ in 0..content_h.saturating_sub(lines.len().saturating_sub(scroll)) {
                            s.push_str("\n");
                        }
                    } else {
                        s.push_str("\n\n\n"); // Placeholder for empty
                    }
                    s.push_str(&format!("{}\n", self.0.border_style.render(&"-".repeat(self.0.terminal_width))));
                    
                    // --- Relations List Area ---
                    s.push_str(&self.0.relations_list.view());
                    
                    // --- Footer ---
                    s.push_str(&format!("\n[Tab] Focus | [.] Next | [,] Prev | [p] Parent | [BS] Back | [Esc] Top | Focus: {:?}", self.0.focus));
                }
            }
        }
        s
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Resolve paths or download if missing
    let model_cache = [
        ".",
        "data/models",
    ].iter().find(|p| std::path::Path::new(p).exists()).cloned()
    .unwrap_or("data/models");

    let downloader = rust_eventbus::embedding::downloader::ModelDownloader::new(model_cache);
    let (model_path, tokenizer_path) = downloader.get_bge_small().await?;

    let state_path = [
        "graph_state.json",
        "examples/outputs/graph_state.json",
    ].iter().find(|p| std::path::Path::new(p).exists()).cloned()
    .unwrap_or("examples/outputs/graph_state.json");
    
    if !std::path::Path::new(&state_path).exists() {
        println!("Error: Graph state not found at {}.", state_path);
        return Ok(());
    }

    // 2. Initialize Service and State
    let onnx_service = Arc::new(OnnxEmbeddingService::new(&model_path, &tokenizer_path)?);
    let json = std::fs::read_to_string(state_path)?;
    let state: rust_eventbus::graph::GraphState = serde_json::from_str(&json)?;
    let state = Arc::new(state);

    ONNX_SERVICE.set(onnx_service).ok();
    GRAPH_STATE.set(state).ok();

    let program = Program::<GlobalModel>::builder()
        .alt_screen(true)
        .build()?;
    program.run().await?;
    
    Ok(())
}
