import React, { useState, useEffect, useRef } from 'react';
import { useMutation } from '@tanstack/react-query';
import { Search, Globe, ChevronDown, ChevronUp, Loader2, Sparkles, FileText, Database, Zap } from 'lucide-react';
import ReactMarkdown from 'react-markdown';

function App() {
  const [crawlUrl, setCrawlUrl] = useState('');
  const [projectId, setProjectId] = useState('demo-project');
  const [maxCrawls, setMaxCrawls] = useState(500);
  const [maxChunks, setMaxChunks] = useState(50);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [isCrawling, setIsCrawling] = useState(false);
  const [expandedItems, setExpandedItems] = useState(new Set());
  const [progress, setProgress] = useState(null);
  const [currentChunk, setCurrentChunk] = useState(null);
  const [crawlComplete, setCrawlComplete] = useState(false);
  const logsRef = useRef(null);
  const logs = useRef([]);
  const [displayLogs, setDisplayLogs] = useState([]);

  useEffect(() => {
    if (logsRef.current) {
      logsRef.current.scrollTop = logsRef.current.scrollHeight;
    }
  }, [displayLogs]);

  const addLog = (msg) => {
    logs.current.push(msg);
    setDisplayLogs([...logs.current]);
  };

  const toggleExpand = (id) => {
    setExpandedItems(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const crawlMutation = useMutation({
    mutationFn: async ({ url, projectId, maxCrawls, maxChunks }) => {
      return new Promise((resolve, reject) => {
        setIsCrawling(true);
        setCrawlComplete(false);
        setProgress(null);
        setCurrentChunk(null);
        logs.current = [];
        setDisplayLogs([]);
        
        const sessionId = Math.random().toString(36).substring(2, 10);
        addLog(`[🚀] Starting crawl: ${url}`);
        addLog(`[📁] Project: ${projectId}`);
        addLog(`[⚙️] Config: max_crawls=${maxCrawls}, max_chunks=${maxChunks}`);
        addLog(`[🔖] Client Session: ${sessionId}`);
        
        const eventSource = new EventSource(`/crawl?url=${encodeURIComponent(url)}&project_id=${encodeURIComponent(projectId)}&max_crawls=${maxCrawls}&max_chunks=${maxChunks}`);

        eventSource.onopen = () => {
          addLog(`[✓] Connected to event stream`);
        };

        eventSource.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            
            if (data.type === 'progress') {
              setProgress(data);
              if (data.session_id) {
                addLog(`[📊] Progress (session: ${data.session_id.substring(0,8)}): ${data.embedded_nodes}/${data.total_nodes} embedded • ${data.ingested_urls}/${data.requested_urls} URLs`);
              } else {
                addLog(`[📊] Progress: ${data.embedded_nodes}/${data.total_nodes} embedded • ${data.ingested_urls}/${data.requested_urls} URLs`);
              }
            } else if (data.Graph && data.Graph.NodeCreated) {
              const nodeId = data.Graph.NodeCreated.id;
              setCurrentChunk({ id: nodeId, metadata: data.Graph.NodeCreated.metadata });
              addLog(`[+] Node: ${nodeId.split('#')[0]}`);
            } else if (data.Graph && data.Graph.EdgeAdded) {
              const { from, to, relation } = data.Graph.EdgeAdded;
              if (relation === 'part_of') {
                addLog(`[🔗] Chunk → Page`);
              } else {
                addLog(`[↔] ${from.split('#')[0]} → ${to.split('#')[0]}`);
              }
            } else if (data.Embedding && data.Embedding.EmbeddingExtracted) {
              addLog(`[✨] Embedded: ${data.Embedding.EmbeddingExtracted.id.split('#')[0]}`);
            }
          } catch (e) {
            console.error("Error parsing SSE data", e);
          }
        };

        eventSource.onerror = (error) => {
          console.log("EventSource closed", error);
          eventSource.close();
          setIsCrawling(false);
          setCrawlComplete(true);
          addLog(`[✅] Crawl session complete`);
          resolve();
        };
      });
    },
  });

  const searchMutation = useMutation({
    mutationFn: async ({ query, projectId }) => {
      const response = await fetch('/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, project_id: projectId }),
      });
      if (!response.ok) throw new Error('Failed to perform search');
      return response.json();
    },
    onSuccess: (data) => setSearchResults(data),
  });

  const handleCrawl = (e) => {
    e.preventDefault();
    if (!crawlUrl.trim()) return;
    crawlMutation.mutate({ url: crawlUrl, projectId, maxCrawls, maxChunks });
  };

  const handleSearch = (e) => {
    e.preventDefault();
    if (!searchQuery.trim()) return;
    searchMutation.mutate({ query: searchQuery, projectId });
  };

  const progressPercent = progress && progress.total_nodes > 0 
    ? Math.round((progress.embedded_nodes / progress.total_nodes) * 100) 
    : 0;

  const isEmbeddingComplete = progress && progress.embedded_nodes >= progress.total_nodes && progress.total_nodes > 0;

  return (
    <div className="min-h-screen bg-[#0a0a0f] text-[#e4e4e7] font-sans">
      {/* Animated background */}
      <div className="fixed inset-0 -z-10 overflow-hidden pointer-events-none">
        <div className="absolute top-0 left-1/4 w-96 h-96 bg-[#6366f1]/5 rounded-full blur-3xl" />
        <div className="absolute bottom-1/4 right-1/4 w-96 h-96 bg-[#8b5cf6]/5 rounded-full blur-3xl" />
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-[#06b6d4]/3 rounded-full blur-3xl" />
      </div>

      {/* Navbar */}
      <header className="border-b border-[#27272a] bg-[#0a0a0f]/80 backdrop-blur-md px-8 py-5 flex items-center justify-between sticky top-0 z-50">
        <div className="flex items-center gap-4">
          <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-indigo-500 via-purple-500 to-cyan-500 flex items-center justify-center shadow-lg shadow-indigo-500/20">
            <Globe className="text-white w-5 h-5" />
          </div>
          <div>
            <h1 className="text-xl font-bold bg-gradient-to-r from-white to-gray-400 bg-clip-text text-transparent">
              Graph Ingest
            </h1>
            <p className="text-xs text-gray-500">Knowledge graph crawler</p>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-[#18181b] border border-[#27272a]">
            <Database size={14} className="text-purple-400" />
            <input
              type="text"
              value={projectId}
              onChange={(e) => setProjectId(e.target.value)}
              className="bg-transparent text-sm text-gray-300 outline-none w-32 placeholder:text-gray-600"
              placeholder="project_id"
            />
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto mt-8 px-4 grid grid-cols-1 lg:grid-cols-12 gap-8 pb-16">
        
        {/* Left Column: Ingestion */}
        <div className="lg:col-span-5 flex flex-col gap-6">
          
          {/* URL Input */}
          <div className="relative rounded-2xl bg-[#18181b]/80 backdrop-blur border border-[#27272a] p-1">
            <form onSubmit={handleCrawl} className="flex gap-2">
              <div className="flex-1 relative">
                <Globe className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-500 w-5 h-5" />
                <input
                  type="url"
                  value={crawlUrl}
                  onChange={(e) => setCrawlUrl(e.target.value)}
                  placeholder="https://example.com"
                  className="w-full bg-[#0a0a0f] border border-[#27272a] rounded-xl px-12 py-4 text-gray-200 placeholder:text-gray-600 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/50 outline-none transition-all"
                />
              </div>
              <button
                type="submit"
                disabled={!crawlUrl.trim() || isCrawling}
                className="px-6 py-4 rounded-xl font-medium text-white bg-gradient-to-r from-indigo-600 to-purple-600 hover:from-indigo-500 hover:to-purple-500 disabled:opacity-40 disabled:cursor-not-allowed transition-all shadow-lg shadow-indigo-500/25"
              >
                {isCrawling ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Ingest'}
              </button>
            </form>
          </div>

          {/* Crawl Config */}
          <div className="rounded-2xl bg-[#18181b]/80 backdrop-blur border border-[#27272a] p-4">
            <div className="flex items-center gap-2 mb-3">
              <Zap size={14} className="text-indigo-400" />
              <span className="text-sm font-medium text-gray-300">Crawl Settings</span>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="text-xs text-gray-500 block mb-1">Max URLs</label>
                <input
                  type="number"
                  value={maxCrawls}
                  onChange={(e) => setMaxCrawls(Math.max(1, parseInt(e.target.value) || 500))}
                  className="w-full bg-[#0a0a0f] border border-[#27272a] rounded-lg px-3 py-2 text-sm text-gray-200 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/50 outline-none"
                  min="1"
                  max="10000"
                />
              </div>
              <div>
                <label className="text-xs text-gray-500 block mb-1">Max Chunks</label>
                <input
                  type="number"
                  value={maxChunks}
                  onChange={(e) => setMaxChunks(Math.max(1, parseInt(e.target.value) || 50))}
                  className="w-full bg-[#0a0a0f] border border-[#27272a] rounded-lg px-3 py-2 text-sm text-gray-200 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/50 outline-none"
                  min="1"
                  max="1000"
                />
              </div>
            </div>
          </div>

          {/* Progress Card */}
          {isCrawling && progress && (
            <div className="rounded-2xl bg-[#18181b]/80 backdrop-blur border border-[#27272a] p-6 shadow-xl">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-2">
                  <div className="w-8 h-8 rounded-lg bg-indigo-500/20 flex items-center justify-center">
                    <Zap className="text-indigo-400 w-4 h-4" />
                  </div>
                  <span className="font-medium text-gray-200">Processing</span>
                </div>
                <span className="text-2xl font-bold bg-gradient-to-r from-indigo-400 to-cyan-400 bg-clip-text text-transparent">
                  {progressPercent}%
                </span>
              </div>
              
              {/* Progress Bar */}
              <div className="h-3 bg-[#27272a] rounded-full overflow-hidden mb-4">
                <div 
                  className="h-full bg-gradient-to-r from-indigo-500 via-purple-500 to-cyan-500 rounded-full transition-all duration-500 ease-out"
                  style={{ width: `${progressPercent}%` }}
                />
              </div>

              {/* Stats */}
              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 rounded-xl bg-[#0a0a0f] border border-[#27272a]">
                  <p className="text-xs text-gray-500 mb-1">Nodes Created</p>
                  <p className="text-lg font-semibold text-white">{progress.total_nodes}</p>
                </div>
                <div className="p-3 rounded-xl bg-[#0a0a0f] border border-[#27272a]">
                  <p className="text-xs text-gray-500 mb-1">Embedded</p>
                  <p className={`text-lg font-semibold ${isEmbeddingComplete ? 'text-green-400' : 'text-cyan-400'}`}>
                    {progress.embedded_nodes}
                  </p>
                </div>
                <div className="p-3 rounded-xl bg-[#0a0a0f] border border-[#27272a]">
                  <p className="text-xs text-gray-500 mb-1">URLs Crawled</p>
                  <p className="text-lg font-semibold text-white">{progress.ingested_urls}</p>
                </div>
                <div className="p-3 rounded-xl bg-[#0a0a0f] border border-[#27272a]">
                  <p className="text-xs text-gray-500 mb-1">URLs Queued</p>
                  <p className="text-lg font-semibold text-white">{progress.requested_urls}</p>
                </div>
              </div>
            </div>
          )}

          {/* Complete State */}
          {crawlComplete && progress && (
            <div className="rounded-2xl bg-gradient-to-br from-green-900/20 to-emerald-900/10 border border-green-500/30 p-6">
              <div className="flex items-center gap-3 mb-3">
                <div className="w-10 h-10 rounded-full bg-green-500/20 flex items-center justify-center">
                  <Sparkles className="text-green-400 w-5 h-5" />
                </div>
                <div>
                  <h3 className="font-semibold text-green-400">Crawl Complete</h3>
                  <p className="text-sm text-gray-400">{progress.total_nodes} nodes indexed</p>
                </div>
              </div>
            </div>
          )}

          {/* Current Chunk Preview */}
          {currentChunk && (
            <div className="rounded-2xl bg-[#18181b]/80 backdrop-blur border border-[#27272a] overflow-hidden">
              <div className="bg-gradient-to-r from-indigo-900/30 to-purple-900/30 px-5 py-3 border-b border-[#27272a] flex items-center gap-2">
                <FileText className="text-indigo-400 w-4 h-4" />
                <span className="text-sm font-medium text-gray-200">Current Chunk</span>
                <span className="ml-auto text-xs text-gray-500 font-mono">
                  {currentChunk.id.split('#chunk-')[1] || '0'}
                </span>
              </div>
              <div className="p-5">
                {currentChunk.metadata?.page_title && (
                  <h4 className="text-sm font-semibold text-white mb-2">{currentChunk.metadata.page_title}</h4>
                )}
                {currentChunk.metadata?.section && (
                  <p className="text-xs text-purple-400 mb-3 font-mono">{currentChunk.metadata.section}</p>
                )}
                <p className="text-sm text-gray-400 line-clamp-4 leading-relaxed">
                  {currentChunk.metadata?.content || 'Processing...'}
                </p>
              </div>
            </div>
          )}

          {/* Live Logs */}
          <div className="rounded-2xl bg-[#18181b]/80 backdrop-blur border border-[#27272a] flex flex-col flex-1 overflow-hidden min-h-[200px]">
            <div className="bg-gradient-to-r from-slate-900/50 to-gray-900/50 px-5 py-3 border-b border-[#27272a] flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
              <span className="text-sm font-medium text-gray-300">Live Events</span>
            </div>
            <div 
              ref={logsRef}
              className="p-4 flex-1 overflow-y-auto font-mono text-xs text-gray-500 max-h-[300px] space-y-1"
            >
              {displayLogs.length === 0 ? (
                <span className="opacity-30">Waiting for events...</span>
              ) : (
                displayLogs.map((log, i) => (
                  <div key={i} className="truncate hover:text-gray-300 transition-colors">
                    {log}
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        {/* Right Column: Search */}
        <div className="lg:col-span-7 flex flex-col gap-6">
          
          {/* Search Input */}
          <div className="relative rounded-2xl bg-[#18181b]/80 backdrop-blur border border-[#27272a] p-1">
            <form onSubmit={handleSearch} className="flex gap-2">
              <div className="flex-1 relative">
                <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-500 w-5 h-5" />
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Search your knowledge graph..."
                  className="w-full bg-[#0a0a0f] border border-[#27272a] rounded-xl px-12 py-4 text-gray-200 placeholder:text-gray-600 focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500/50 outline-none transition-all"
                />
              </div>
              <button
                type="submit"
                disabled={searchMutation.isPending || !searchQuery.trim()}
                className="px-6 py-4 rounded-xl font-medium text-white bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 disabled:opacity-40 disabled:cursor-not-allowed transition-all shadow-lg shadow-cyan-500/25"
              >
                {searchMutation.isPending ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Search'}
              </button>
            </form>
          </div>

          {/* Search Results */}
          <div className="rounded-2xl bg-[#18181b]/80 backdrop-blur border border-[#27272a] overflow-hidden flex flex-col flex-1">
            <div className="bg-gradient-to-r from-slate-900/50 to-gray-900/50 px-5 py-4 border-b border-[#27272a] flex justify-between items-center">
              <h3 className="text-sm font-medium text-gray-300 flex items-center gap-2">
                <Search size={16} className="text-cyan-400" />
                Results
              </h3>
              <span className="text-xs text-gray-500 bg-[#0a0a0f] px-2 py-1 rounded-md border border-[#27272a]">
                {searchResults.length} found
              </span>
            </div>
            <div className="flex-1 overflow-y-auto divide-y divide-[#27272a] max-h-[700px]">
              {searchResults.length === 0 ? (
                <div className="p-16 text-center">
                  <div className="w-16 h-16 mx-auto mb-4 rounded-2xl bg-[#18181b] border border-[#27272a] flex items-center justify-center">
                    <Search className="text-gray-600 w-8 h-8" />
                  </div>
                  <p className="text-gray-500">No results found. Try ingesting some data first.</p>
                </div>
              ) : (
                searchResults.map((result, index) => {
                  const isExpanded = expandedItems.has(result.id);
                  return (
                    <div key={index} className="p-5 hover:bg-[#1f1f23] transition-colors">
                      <div className="flex justify-between items-start mb-3">
                        <div 
                          onClick={() => toggleExpand(result.id)}
                          className="flex-1 cursor-pointer group"
                        >
                          <h4 className="font-medium text-gray-200 group-hover:text-cyan-400 transition-colors flex items-center gap-2">
                            {result.title}
                            {isExpanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                          </h4>
                        </div>
                        <div className="flex gap-2 ml-4">
                          <span className="text-xs px-2 py-1 rounded-md bg-green-900/30 text-green-400 border border-green-500/30">
                            {((result.score || result.vector_score * 0.7 + (result.text_score || 0) * 0.3) * 100).toFixed(0)}%
                          </span>
                        </div>
                      </div>
                      
                      {result.section && (
        <p className="text-xs text-purple-400 mb-2 font-mono bg-purple-900/10 px-2 py-0.5 rounded inline-block">
          {result.section}
        </p>
      )}
                      
                      {isExpanded ? (
                        <div className="mt-4 p-4 bg-[#0a0a0f] rounded-xl border border-[#27272a] text-sm text-gray-300 leading-relaxed">
                          <ReactMarkdown>{result.content}</ReactMarkdown>
                        </div>
                      ) : (
                        <p className="text-sm text-gray-500 line-clamp-2 leading-relaxed">
                          {result.content}
                        </p>
                      )}
                      
                      <p className="text-xs text-gray-600 mt-3 font-mono truncate">
                        {result.id}
                      </p>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        </div>

      </main>
    </div>
  );
}

export default App;