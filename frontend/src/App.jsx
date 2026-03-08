import React, { useState, useEffect } from 'react';
import { 
  CheckCircle2, 
  Circle, 
  Trash2, 
  Plus, 
  Search,
  Filter,
  Check,
  ChevronDown,
  Info,
  ExternalLink
} from 'lucide-react';

function App() {
  const [todos, setTodos] = useState([]);
  const [newTodo, setNewTodo] = useState('');
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchTodos = async () => {
    try {
      const response = await fetch('/api/todos');
      if (!response.ok) throw new Error('Failed to fetch todos');
      const data = await response.json();
      setTodos(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchTodos();
  }, []);

  const addTodo = async (e) => {
    e.preventDefault();
    if (!newTodo.trim()) return;

    try {
      const response = await fetch('/api/todos', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: newTodo }),
      });
      if (!response.ok) throw new Error('Failed to add todo');
      setNewTodo('');
      fetchTodos();
    } catch (err) {
      setError(err.message);
    }
  };

  const toggleTodo = async (id) => {
    try {
      const response = await fetch(`/api/todos/${id}/complete`, {
        method: 'PUT',
      });
      if (!response.ok) throw new Error('Failed to complete todo');
      fetchTodos();
    } catch (err) {
      setError(err.message);
    }
  };

  const deleteTodo = async (id) => {
    try {
      const response = await fetch(`/api/todos/${id}`, {
        method: 'DELETE',
      });
      if (!response.ok) throw new Error('Failed to delete todo');
      fetchTodos();
    } catch (err) {
      setError(err.message);
    }
  };

  const openTodos = todos.filter(t => !t.completed);
  const completedTodos = todos.filter(t => t.completed);

  return (
    <div className="min-h-screen bg-github-canvas text-github-textMain font-sans pb-12">
      {/* Navbar */}
      <header className="bg-github-header border-b border-github-border px-8 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <div className="w-8 h-8 rounded-full bg-[#30363d] flex items-center justify-center">
            <Check className="text-github-textMain w-5 h-5" />
          </div>
          <h1 className="text-xl font-semibold">EventBus Todos</h1>
        </div>
        <div className="flex items-center gap-4 text-github-textSecondary text-sm">
          <span>v1.0.0</span>
          <a href="https://todo.k6n.net" target="_blank" rel="noreferrer" className="hover:text-github-accent flex items-center gap-1">
            API <ExternalLink size={14} />
          </a>
        </div>
      </header>

      <main className="max-w-4xl mx-auto mt-10 px-4">
        {/* Input Section */}
        <form onSubmit={addTodo} className="mb-8 flex gap-2">
          <input
            type="text"
            value={newTodo}
            onChange={(e) => setNewTodo(e.target.value)}
            placeholder="What needs to be done?"
            className="flex-1 bg-github-bg border border-github-border rounded-md px-4 py-2 focus:border-github-accent focus:ring-1 focus:ring-github-accent outline-none transition-all placeholder:text-github-textSecondary"
          />
          <button
            type="submit"
            className="bg-github-success hover:bg-opacity-90 text-white font-medium px-4 py-2 rounded-md flex items-center gap-2 transition-colors shadow-sm"
          >
            <Plus size={18} /> New Todo
          </button>
        </form>

        {error && (
          <div className="bg-red-900/20 border border-red-500/50 text-red-200 px-4 py-3 rounded-md mb-6 flex items-center gap-3">
            <Info size={18} />
            <p>{error}</p>
          </div>
        )}

        {/* Issues List Container */}
        <div className="border border-github-border rounded-md bg-github-bg overflow-hidden shadow-sm">
          {/* Header */}
          <div className="bg-github-header border-b border-github-border p-4 flex items-center justify-between">
            <div className="flex items-center gap-4">
              <button 
                className={`flex items-center gap-1 text-sm font-semibold hover:text-github-textMain transition-colors ${openTodos.length > 0 ? 'text-github-textMain' : 'text-github-textSecondary'}`}
              >
                <Circle size={16} className="text-github-open" /> {openTodos.length} Open
              </button>
              <button 
                className={`flex items-center gap-1 text-sm font-medium hover:text-github-textMain transition-colors ${completedTodos.length > 0 ? 'text-github-textMain' : 'text-github-textSecondary'}`}
              >
                <CheckCircle2 size={16} className="text-github-closed" /> {completedTodos.length} Completed
              </button>
            </div>
            <div className="flex items-center gap-4 text-sm text-github-textSecondary">
              <div className="group cursor-pointer flex items-center gap-1 hover:text-github-accent">
                Sort <ChevronDown size={14} />
              </div>
            </div>
          </div>

          {/* List Items */}
          <div className="divide-y divide-github-border">
            {isLoading ? (
              <div className="p-8 text-center text-github-textSecondary flex flex-col items-center gap-4">
                <div className="w-8 h-8 border-2 border-github-accent border-t-transparent rounded-full animate-spin"></div>
                <p>Loading your tasks...</p>
              </div>
            ) : todos.length === 0 ? (
              <div className="p-16 text-center flex flex-col items-center gap-4">
                <div className="text-github-textSecondary">
                  <Info size={48} className="mx-auto opacity-20 mb-4" />
                  <p className="text-lg font-semibold text-github-textMain">No todos found</p>
                  <p className="text-sm">Add a task above to get started.</p>
                </div>
              </div>
            ) : (
              todos.map((todo) => (
                <div 
                  key={todo.id} 
                  className="p-4 hover:bg-github-itemHover flex items-start gap-4 transition-colors group"
                >
                  <button 
                    onClick={() => toggleTodo(todo.id)}
                    className={`mt-1 transition-colors ${todo.completed ? 'text-github-closed' : 'text-github-open hover:text-github-accent'}`}
                  >
                    {todo.completed ? <CheckCircle2 size={18} /> : <Circle size={18} />}
                  </button>
                  
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <h3 className={`font-semibold text-lg leading-tight truncate ${todo.completed ? 'text-github-textSecondary line-through' : 'text-github-textMain'}`}>
                        {todo.title}
                      </h3>
                      {todo.completed && (
                        <span className="bg-github-closed/10 text-github-closed border border-github-closed/20 text-[10px] uppercase font-bold px-1.5 py-0.5 rounded-full">
                          Done
                        </span>
                      )}
                    </div>
                    <div className="flex items-center gap-2 text-xs text-github-textSecondary">
                      <span>#{todo.id.split('-')[0]}</span>
                      <span>•</span>
                      <span>created via rust-eventbus</span>
                    </div>
                  </div>

                  <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                    <button 
                      onClick={() => deleteTodo(todo.id)}
                      className="p-2 hover:bg-red-500/10 hover:text-red-400 rounded-md transition-all text-github-textSecondary"
                      title="Delete todo"
                    >
                      <Trash2 size={16} />
                    </button>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Footer Info */}
        <div className="mt-8 text-center text-xs text-github-textSecondary">
          <p>© 2026 Antigravity Systems. Inspired by GitHub Issues.</p>
        </div>
      </main>
    </div>
  );
}

export default App;
