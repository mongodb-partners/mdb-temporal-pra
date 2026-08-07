import { useState } from "react";
import type { AgentResponse } from "./types";

const EXAMPLES = [
  "How do I start a Temporal worker in Python?",
  "What is a Temporal Workflow and how does it work?",
  "How do Signals and Queries differ in Temporal?",
];

// Temporal Web UI (dev server). Used to deep-link the durable agent workflow run.
const TEMPORAL_UI = "http://localhost:8233";

function workflowUrl(id: string): string {
  return `${TEMPORAL_UI}/namespaces/default/workflows/${encodeURIComponent(id)}`;
}

export default function App() {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [res, setRes] = useState<AgentResponse | null>(null);

  async function run(q: string) {
    const question = q.trim();
    if (!question || loading) return;
    setLoading(true);
    setError(null);
    try {
      const r = await fetch("/research", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: question }),
      });
      if (!r.ok) {
        let detail = `API ${r.status}`;
        try {
          const body = await r.json();
          if (body?.detail) detail = body.detail;
        } catch {
          // non-JSON error body — keep the status message
        }
        throw new Error(detail);
      }
      setRes(await r.json());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  const toolCalls = res?.tool_calls ?? [];

  return (
    <div className="app">
      <header>
        <h1>Temporal Docs — Research Agent</h1>
        <p className="sub">Durable OpenAI agent on Temporal · vector search + rerank tools over MongoDB Atlas</p>
      </header>

      <form
        className="searchbar"
        onSubmit={(e) => {
          e.preventDefault();
          run(query);
        }}
      >
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Ask a question about the Temporal documentation…"
          autoFocus
        />
        <button type="submit" disabled={loading || !query.trim()}>
          {loading ? "Researching…" : "Ask"}
        </button>
      </form>

      <div className="examples">
        {EXAMPLES.map((ex) => (
          <button key={ex} className="chip" onClick={() => { setQuery(ex); run(ex); }}>
            {ex}
          </button>
        ))}
      </div>

      {loading && (
        <div className="note">
          Running the agent as a durable Temporal workflow… the full answer returns when the
          agent finishes (results are not streamed).
        </div>
      )}

      {error && <div className="error">Error: {error}</div>}

      {res && !loading && (
        <div className="results">
          <div className="meta">
            model <code>{res.model}</code> ·{" "}
            <a href={workflowUrl(res.workflow_id)} target="_blank" rel="noreferrer">
              view workflow in Temporal UI ↗
            </a>
          </div>

          {res.answer ? (
            <div className="answer">
              <div className="answer-head">
                <h2>Answer</h2>
              </div>
              <div className="answer-body">{res.answer}</div>
            </div>
          ) : (
            <div className="note">The agent did not return an answer.</div>
          )}

          {toolCalls.length > 0 && (
            <div className="trajectory">
              <h3>What the agent did</h3>
              <ol className="tool-calls">
                {toolCalls.map((t, i) => (
                  <li key={i}><code>{t}</code></li>
                ))}
              </ol>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
