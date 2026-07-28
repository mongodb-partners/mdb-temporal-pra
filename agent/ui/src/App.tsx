import { useState } from "react";
import type { QueryResponse, Source } from "./types";

const EXAMPLES = [
  "How do I start a Temporal worker in Python?",
  "What is a Temporal Workflow and how does it work?",
  "How do Signals and Queries differ in Temporal?",
];

function basename(uri: string): string {
  return uri.split("/").pop() || uri;
}

function extractCitedSourceIndexes(answer: string | null): number[] {
  if (!answer) return [];
  const seen = new Set<number>();
  const regex = /\[(\d+)\]/g;
  let match: RegExpExecArray | null = regex.exec(answer);
  while (match) {
    const idx = Number.parseInt(match[1], 10);
    if (idx > 0) seen.add(idx);
    match = regex.exec(answer);
  }
  return [...seen];
}

function extractFirstHttpUrl(text: string): string | null {
  const match = text.match(/https?:\/\/[^\s\])>]+/i);
  if (!match) return null;
  return match[0].replace(/[.,;:)]+$/, "");
}

function pickPrimarySource(res: QueryResponse): Source | null {
  const cited = extractCitedSourceIndexes(res.answer)
    .map((n) => res.sources.find((s) => s.n === n))
    .filter((s): s is Source => Boolean(s));

  if (cited.length > 0) return cited[0];
  if (res.sources.length > 0) return res.sources[0];
  return null;
}

function buildPrimaryReference(res: QueryResponse): { label: string; href: string | null } | null {
  const source = pickPrimarySource(res);
  if (!source) return null;

  const fileName = basename(source.s3_uri);
  if (source.s3_uri.startsWith("http://") || source.s3_uri.startsWith("https://")) {
    return { label: fileName, href: source.s3_uri };
  }

  const articleUrl = extractFirstHttpUrl(source.text);
  if (articleUrl) {
    return { label: fileName, href: articleUrl };
  }

  return { label: fileName, href: null };
}

export default function App() {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [res, setRes] = useState<QueryResponse | null>(null);

  async function run(q: string) {
    const question = q.trim();
    if (!question || loading) return;
    setLoading(true);
    setError(null);
    try {
      const r = await fetch("/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: question, k: 12, top_k: 5 }),
      });
      if (!r.ok) throw new Error(`API ${r.status}`);
      setRes(await r.json());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="app">
      <header>
        <h1>Fresh Vector Store — Deep Agent</h1>
        <p className="sub">Temporal-ingested knowledge in MongoDB Atlas · Voyage embeddings + rerank</p>
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
          placeholder="Ask a question about the ingested documents…"
          autoFocus
        />
        <button type="submit" disabled={loading || !query.trim()}>
          {loading ? "Searching…" : "Ask"}
        </button>
      </form>

      <div className="examples">
        {EXAMPLES.map((ex) => (
          <button key={ex} className="chip" onClick={() => { setQuery(ex); run(ex); }}>
            {ex}
          </button>
        ))}
      </div>

      {error && <div className="error">Error: {error}</div>}

      {res && (
        <div className="results">
          <div className="meta">
            active collection <code>{res.active_collection}</code> · embedding <code>{res.model}</code>
          </div>

          {res.answer_available ? (
            <div className="answer">
              <div className="answer-head">
                <h2>Answer</h2>
                <div className="answer-links">
                  {(() => {
                    const ref = buildPrimaryReference(res);
                    if (!ref) return null;
                    if (!ref.href) {
                      return <span className="source-file-label" title={ref.label}>{ref.label}</span>;
                    }
                    return (
                      <a
                        className="source-link-icon"
                        href={ref.href}
                        title={`Open article: ${ref.label}`}
                        target="_blank"
                        rel="noreferrer"
                      >
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                          <path d="M14 3h7v7h-2V6.41l-9.29 9.3-1.42-1.42 9.3-9.29H14V3z" />
                          <path d="M5 5h6v2H7v10h10v-4h2v6H5V5z" />
                        </svg>
                      </a>
                    );
                  })()}
                </div>
              </div>
              <div className="answer-body">{res.answer}</div>
            </div>
          ) : (
            <div className="note">
              No LLM answer available right now. Check API key/gateway configuration to enable synthesis.
            </div>
          )}
        </div>
      )}
    </div>
  );
}
