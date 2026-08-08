import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Proxy API calls to the FastAPI deep-agent backend (agent/api.py on :8090).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/research": "http://localhost:8090",
      "/health": "http://localhost:8090",
    },
  },
});
