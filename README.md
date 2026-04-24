# Dataset Curation Workbench

Web UI + FastAPI for SFT dataset curation (stages, filters, signature exploration). Each **task** is persisted under **`DATA_DIR`** (default `./data`): SQLite metadata (`tasks.db`) plus JSONL files under `tasks/{task_id}/` (raw upload, per-stage kept/removed, filter log).

## Architecture

- **Local development:** Vite dev server (e.g. port 5173) + FastAPI (port 8000). The UI calls `/api/...` on the Vite origin; Vite **proxies** `/api` to `http://127.0.0.1:8000`.
- **Production (single URL):** one FastAPI process serves the React static build at `/` and all API routes under `/api/...` (see `scripts/start_production.sh`). The same process reads/writes **`DATA_DIR`**; use a mounted volume on PaaS so tasks survive restarts.

### Environment

| Variable | Purpose |
|----------|---------|
| `DATA_DIR` | Root for SQLite + task folders (default `./data`). |
| `DATABASE_URL` | Optional; only **SQLite** URLs are supported today. If unset, the app uses `DATA_DIR/tasks.db`. |

## Requirements

- **Python** 3.10+ (3.11 recommended for hosting)
- **Node.js** 20+ and npm (for building the frontend)

## Local development (unchanged workflow: two processes)

### 1. API

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

- API base path: **`/api`** (e.g. health: `http://127.0.0.1:8000/api/health`, version: `http://127.0.0.1:8000/api/version`).

### 2. Frontend

```bash
cd frontend
npm install
npm run dev
```

Open the URL Vite prints (usually `http://127.0.0.1:5173`). No `.env` is required: the client uses **`/api`** and the Vite proxy forwards to the backend.

**Optional:** to call the API directly (no proxy), create `frontend/.env` from `frontend/.env.example` and set e.g. `VITE_API_BASE_URL=http://127.0.0.1:8000/api`.

**Remote API on the LAN:** set `VITE_API_BASE_URL=http://<host>:8000/api` and restart `npm run dev`.

## Production: one process, one URL

From the **repository root** (after installing Python dependencies in a venv or globally):

```bash
pip install -r backend/requirements.txt
bash scripts/start_production.sh
```

This builds `frontend/dist` if missing, then runs Uvicorn on `0.0.0.0` (port **`8000`**, or **`$PORT`** if set). Open `http://localhost:8000` — the same host serves the UI and `/api/...`.

The production build uses **relative** `/api` (no localhost in the client bundle).

## Deployment

1. Push this repository to GitHub.
2. On [Render](https://render.com) (or similar), create a **Web Service** from the repo.
3. **Build command:**  
   `pip install -r backend/requirements.txt && bash scripts/build_frontend.sh`
4. **Start command:**  
   `bash scripts/start_production.sh`
5. Set **environment** if needed: `DATA_DIR` (e.g. `/var/data` on a persistent disk), `APP_VERSION`, `BUILD_TIME` (optional; exposed by `GET /api/version`). Render sets `PORT` automatically.
6. Deploy and open the public URL — users only need that link; they do not run backend and frontend separately.

A sample [Render Blueprint](https://render.com/docs/blueprint-spec) is in `render.yaml` (adjust names/region to match your account).

## API prefix

Task-scoped routes are under **`/api/tasks/...`**, for example:

- `POST /api/tasks` — create a task (returns `task_id`)
- `POST /api/tasks/{task_id}/datasets/upload` — upload JSONL into that task
- `GET /api/tasks/{task_id}/stages`
- `POST /api/tasks/{task_id}/apply-filters`
- `GET /api/tasks/{task_id}/export?...`
- `GET /api/version`, `GET /api/health`, `GET /api/filters`

## Pinning versions

- Backend: `backend/requirements.txt` (use `pip freeze` for a full lock if needed).
- Frontend: use `npm ci` when `package-lock.json` is present.
