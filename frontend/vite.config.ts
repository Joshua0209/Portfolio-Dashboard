import { defineConfig } from "vite";

// API proxy target: during the Phase 8/9 transition the new frontend
// talks to the legacy Flask backend on :8000. Phase 9 cutover stands up
// the FastAPI port on :8001 — flip the target then. Override at dev
// time with VITE_API_TARGET=http://127.0.0.1:8001 npm run dev.
const API_TARGET = process.env.VITE_API_TARGET ?? "http://127.0.0.1:8000";

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
