export interface StartResponse {
  workflow_id: string;
}

export interface ProgressResponse {
  workflow_id: string;
  status?: string;
  steps: string[];
  tool_calls: string[];
  answer: string | null;
  model: string | null;
  done: boolean;
}
