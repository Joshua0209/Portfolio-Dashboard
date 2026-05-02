import { defineConfig } from "vite";

// API proxy target: post-Phase 9 cutover the new FastAPI backend is
// canonical on :8001. Override at dev time with
// VITE_API_TARGET=http://other:port npm run dev.
const API_TARGET = process.env.VITE_API_TARGET ?? "http://127.0.0.1:8001";

export default defineConfig({
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: API_TARGET,
        changeOrigin: false,
      },
    },
  },
  build: {
    target: "es2022",
    sourcemap: true,
    outDir: "dist",
    emptyOutDir: true,
  },
});
