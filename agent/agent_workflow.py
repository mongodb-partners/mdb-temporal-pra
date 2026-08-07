"""DeepResearchAgent — a durable OpenAI Agents SDK agent over the Temporal docs.

The agent's reasoning loop runs inside this Temporal workflow; the model calls and the
vector_search / rerank tools execute as activities, so the whole trajectory is durable and
auditable in the Temporal UI. A hosted web-search tool supplements the docs — it runs inside
the model-call activity (OpenAI Responses API), not as a separate activity. Contrast with the
fixed `agent/retrieval.py` chain.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agents import Agent, Runner, WebSearchTool
    from temporalio.contrib.openai_agents.workflow import activity_as_tool

    from pipeline.config import settings

    from .tools import rerank_tool, vector_search_tool


_INSTRUCTIONS = (
    "You are a precise research assistant answering questions about Temporal, backed by a "
    "MongoDB Atlas knowledge base of Temporal documentation plus web search.\n"
    "- Prefer the ingested docs: decompose any multi-part or comparative question into its "
    "distinct sub-topics and call `vector_search_tool` SEPARATELY for each sub-topic with a "
    "focused query — do not cover several concepts in one broad search. For a genuinely "
    "single-topic question, one search is fine.\n"
    "- If a search returns thin or off-target results, reformulate the query and search again.\n"
    "- Call `rerank_tool` with the collected chunk_ids to prioritize the best chunks before "
    "answering.\n"
    "- Use web search to SUPPLEMENT the docs: for very recent changes, topics the knowledge "
    "base does not cover, or to corroborate a claim. The ingested docs are authoritative for "
    "how Temporal works — prefer them over the open web when they conflict.\n"
    "- Answer from your gathered sources. Cite inline as [n]: give the source_uri for "
    "knowledge-base chunks and the URL for web results, address each sub-topic, and make clear "
    "which claims came from the docs vs the web. If neither contains the answer, say so plainly."
)


@workflow.defn
class DeepResearchAgent:
    @workflow.run
    async def run(self, query: str) -> dict:
        agent = Agent(
            name="Temporal docs researcher",
            model=settings.agent_model,
            instructions=_INSTRUCTIONS,
            tools=[
                activity_as_tool(
                    vector_search_tool, start_to_close_timeout=timedelta(seconds=30)
                ),
                activity_as_tool(rerank_tool, start_to_close_timeout=timedelta(seconds=30)),
                # Hosted tool: runs inside the model-call activity (OpenAI Responses API),
                # not as a separate Temporal activity. Requires a web-search-capable model.
                WebSearchTool(),
            ],
        )
        result = await Runner.run(agent, query, max_turns=settings.agent_max_turns)

        # Best-effort tool-call trajectory (the durable source of truth is the workflow
        # history in the Temporal UI).
        tool_calls: list[str] = []
        try:
            for item in getattr(result, "new_items", []):
                if type(item).__name__ == "ToolCallItem":
                    raw = getattr(item, "raw_item", None)
                    # function tools expose `.name`; hosted tools (web search) expose `.type`
                    label = getattr(raw, "name", None) or getattr(raw, "type", None)
                    if label:
                        tool_calls.append(label)
        except Exception:  # noqa: BLE001 - trajectory is diagnostic only
            tool_calls = []

        return {
            "query": query,
            "answer": result.final_output,
            "model": settings.agent_model,
            "tool_calls": tool_calls,
        }
