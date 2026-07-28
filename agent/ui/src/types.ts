export interface Source {
  n: number;
  s3_uri: string;
  chunk_id: string;
  score: number;
  vector_score: number;
  text: string;
}

export interface QueryResponse {
  query: string;
  answer: string | null;
  answer_available: boolean;
  active_collection: string;
  model: string;
  sources: Source[];
}
