export interface AgentResponse {
  workflow_id: string;
  query: string;
  answer: string | null;
  model: string;
  tool_calls: string[];
}
