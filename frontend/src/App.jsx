import React, { useState, useEffect } from 'react';
import { useMutation } from '@tanstack/react-query';
import { Search, Save, Globe, Info, ChevronDown, ChevronUp } from 'lucide-react';
import ReactMarkdown from 'react-markdown';

function App() {
  const [crawlUrl, setCrawlUrl] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [logs, setLogs] = useState([]);
  const [isCrawling, setIsCrawling] = useState(false);
  const [expandedItems, setExpandedItems] = useState(new Set());

  const toggleExpand = (id) => {
    setExpandedItems(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const crawlMutation = useMutation({
    mutationFn: async (url) => {
      // In SSE, the GET request to the endpoint starts the action
      // We don't need to return a long-running promise here if we handle state via listeners
      return new Promise((resolve, reject) => {
        setIsCrawling(true);
        setLogs([]);
        
        const eventSource = new EventSource(`/api/crawl?url=${encodeURIComponent(url)}`);

        eventSource.onopen = () => {
          setLogs(prev => [...prev, `[System] Connected to crawl stream...`]);
        };

        eventSource.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            if (data.Graph && data.Graph.NodeCreated) {
              setLogs(prev => [...prev, `Created node: ${data.Graph.NodeCreated.id}`]);
            } else if (data.Graph && data.Graph.EdgeAdded) {
              setLogs(prev => [...prev, `Added edge: ${data.Graph.EdgeAdded.from} -> ${data.Graph.EdgeAdded.to}`]);
            } else if (data.Embedding && data.Embedding.EmbeddingExtracted) {
               setLogs(prev => [...prev, `Extracted embedding for: ${data.Embedding.EmbeddingExtracted.id}`]);
            }
          } catch (e) {
            console.error("Error parsing SSE data", e);
          }
        };

        eventSource.onerror = (error) => {
          // SSE usually reconnects, but we want to know when it's logically done or failed
          // The server closes the stream when the individual crawl session is finished
          console.log("EventSource closed or failed", error);
          eventSource.close();
          setIsCrawling(false);
          setLogs(prev => [...prev, `[System] Crawl session finished.`]);
          resolve();
        };
      });
    },
  });

  const saveMutation = useMutation({
    mutationFn: async () => {
      const response = await fetch('/api/save', { method: 'POST' });
      if (!response.ok) throw new Error('Failed to save graph');
      return response.text();
    },
    onSuccess: (data) => alert(data),
    onError: (error) => alert(error.message),
  });

  const searchMutation = useMutation({
    mutationFn: async (query) => {
      const response = await fetch('/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query }),
      });
      if (!response.ok) throw new Error('Failed to perform search');
      return response.json();
    },
    onSuccess: (data) => setSearchResults(data),
  });

  const handleCrawl = (e) => {
    e.preventDefault();
    if (!crawlUrl.trim()) return;
    crawlMutation.mutate(crawlUrl);
  };

  const handleSearch = (e) => {
    e.preventDefault();
    if (!searchQuery.trim()) return;
    searchMutation.mutate(searchQuery);
  };

  return (
    <div className="min-h-screen bg-github-canvas text-github-textMain font-sans pb-12">
      {/* Navbar */}
      <header className="bg-github-header border-b border-github-border px-8 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <div className="w-8 h-8 rounded-full bg-[#30363d] flex items-center justify-center">
            <Globe className="text-github-textMain w-5 h-5" />
          </div>
          <h1 className="text-xl font-semibold">Knowledge Graph Navigator</h1>
        </div>
        <div className="flex items-center gap-6 text-github-textSecondary text-sm">
          <button 
            onClick={() => saveMutation.mutate()}
            disabled={saveMutation.isPending}
            className="text-github-accent hover:text-blue-400 font-medium flex items-center gap-1 transition-colors disabled:opacity-50"
          >
            <Save size={16} /> {saveMutation.isPending ? 'Saving...' : 'Save Graph'}
          </button>
        </div>
      </header>

      <main className="max-w-6xl mx-auto mt-10 px-4 grid grid-cols-1 md:grid-cols-2 gap-8">

        {/* Left Column: Ingestion */}
        <div className="flex flex-col gap-6">
          <div className="border border-github-border rounded-md bg-github-bg p-6 shadow-sm">
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Globe size={20} /> Ingest URL
            </h2>
            <form onSubmit={handleCrawl} className="flex gap-2">
              <input
                type="url"
                value={crawlUrl}
                onChange={(e) => setCrawlUrl(e.target.value)}
                placeholder="https://example.com"
                className="flex-1 bg-github-canvas border border-github-border rounded-md px-4 py-2 focus:border-github-accent outline-none transition-all placeholder:text-github-textSecondary"
              />
              <button
                type="submit"
                disabled={!crawlUrl.trim() || isCrawling}
                className="bg-github-success hover:bg-opacity-90 text-white font-medium px-4 py-2 rounded-md flex items-center gap-2 transition-colors disabled:opacity-50"
              >
                {isCrawling ? 'Crawling...' : 'Ingest'}
              </button>
            </form>
          </div>

          <div className="border border-github-border rounded-md bg-github-bg flex flex-col flex-1 shadow-sm overflow-hidden min-h-[300px]">
            <div className="bg-github-header border-b border-github-border p-3">
              <h3 className="text-sm font-semibold flex items-center gap-2 text-github-textSecondary">
                <Info size={16} /> Live Logs
              </h3>
            </div>
            <div className="p-4 flex-1 overflow-y-auto bg-[#0d1117] font-mono text-xs text-green-400 leading-relaxed max-h-[500px]">
              {logs.length === 0 ? (
                <span className="opacity-50">Waiting for events...</span>
              ) : (
                logs.map((log, i) => <div key={i}>{log}</div>)
              )}
            </div>
          </div>
        </div>

        {/* Right Column: Search */}
        <div className="flex flex-col gap-6">
          <div className="border border-github-border rounded-md bg-github-bg p-6 shadow-sm">
             <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Search size={20} /> Search Graph
            </h2>
            <form onSubmit={handleSearch} className="flex gap-2">
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Ask something..."
                className="flex-1 bg-github-canvas border border-github-border rounded-md px-4 py-2 focus:border-github-accent outline-none transition-all placeholder:text-github-textSecondary"
              />
              <button
                type="submit"
                disabled={searchMutation.isPending || !searchQuery.trim()}
                className="bg-github-accent hover:bg-opacity-90 text-white font-medium px-4 py-2 rounded-md flex items-center gap-2 transition-colors disabled:opacity-50"
              >
                {searchMutation.isPending ? 'Searching...' : 'Search'}
              </button>
            </form>
          </div>

          <div className="border border-github-border rounded-md bg-github-bg overflow-hidden shadow-sm flex-1 flex flex-col">
            <div className="bg-github-header border-b border-github-border p-4 flex justify-between items-center">
              <h3 className="text-sm font-semibold">Search Results</h3>
              <span className="text-xs text-github-textSecondary">{searchResults.length} results found</span>
            </div>
            <div className="flex-1 overflow-y-auto max-h-[600px] divide-y divide-github-border">
              {searchResults.length === 0 ? (
                <div className="p-16 text-center text-github-textSecondary">
                  <p>No results. Try searching or ingesting some data first.</p>
                </div>
              ) : (
                searchResults.map((result, index) => {
                  const isExpanded = expandedItems.has(result.id);
                  return (
                    <div key={index} className="p-4 hover:bg-github-itemHover transition-colors">
                      <div className="flex justify-between items-start mb-2 text-left">
                        <div 
                           onClick={() => toggleExpand(result.id)}
                           className="flex-1 cursor-pointer group"
                        >
                          <h4 className="font-semibold text-[#58a6ff] group-hover:underline flex items-center gap-2">
                            {result.title}
                            {isExpanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                          </h4>
                        </div>
                         <div className="flex gap-2 text-xs">
                            <span className="bg-[#238636] bg-opacity-20 text-[#2ea043] px-2 py-1 rounded border border-[#2ea043]/30">
                              Rerank: {result.rerank_score.toFixed(3)}
                            </span>
                            <span className="bg-[#1f6feb] bg-opacity-20 text-[#58a6ff] px-2 py-1 rounded border border-[#58a6ff]/30">
                              Vector: {result.vector_score.toFixed(3)}
                            </span>
                         </div>
                      </div>
                      {result.section && (
                         <p className="text-xs text-github-textSecondary mb-2 font-mono bg-github-canvas inline-block px-1 rounded">
                           Section: {result.section}
                         </p>
                      )}
                      
                      {isExpanded ? (
                        <div className="mt-3 p-4 bg-github-canvas rounded-md border border-github-border markdown-body text-sm">
                          <ReactMarkdown>{result.content}</ReactMarkdown>
                        </div>
                      ) : (
                        <p className="text-sm text-github-textSecondary line-clamp-3">
                          {result.content}
                        </p>
                      )}
                      
                      <p className="text-xs text-github-textSecondary mt-2 opacity-50 truncate">
                         ID: {result.id}
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
