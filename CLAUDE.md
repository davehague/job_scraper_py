# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Python job recommendation engine that scrapes listings from multiple job boards, scores them against user resumes using LLM-based structured evaluation, stores results in Supabase, and sends email notifications.

Built with Python 3.11.2. This is the **backend/scraper** half of a two-repo system. The **frontend** lives at `/Users/davidhague/source/job_scraper_web` ([GitHub](https://github.com/davehague/job_scraper_web)).

## Running the Application

```bash
# Setup
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt

# Run
python main.py
```

The `SCHEDULED` flag in `main.py` controls whether output goes to a log file (`~/Downloads/job_scraper_YYYY-MM-DD.log`) or stdout.

Set `SMALL_RUN = True` in `main.py` for a quick smoke test. This processes only the first user with 2 results per site, skips existing-job matching and email sending, and forces output to stdout.

## Deploying to GCP

```bash
cd jobs-app-gcp
gcloud config set project <<your-project-id>>
gcloud functions deploy jobs_app_function \
  --runtime python311 --trigger-http --allow-unauthenticated \
  --gen2 --env-vars-file .env.yaml --memory 512M --timeout=180 --source .
```

The GCP function (`jobs-app-gcp/main.py`) is an HTTP-triggered cloud function with API key validation via `X-API-Key` header.

## Environment Variables

Required in `.env` (root) and `.env.yaml` (GCP):

- `OPENROUTER_API_KEY` — LLM provider via OpenRouter (routes to OpenAI, Anthropic, Google, etc.)
- `LLM_MODEL_FAST` — (optional) Model for fast/cheap tasks, default: `openai/gpt-4.1-nano`
- `LLM_MODEL_STRUCTURED` — (optional) Model for structured eval, default: `openai/gpt-5-mini`
- `SUPABASE_URL`, `SUPABASE_KEY` — Database (service account key, bypasses RLS)
- `MJ_APIKEY_PUBLIC`, `MJ_APIKEY_PRIVATE` — Mailjet email sending
- `GOOGLE_CLOUD_FUNCTION_API_KEY` — GCP function auth (GCP only)

## Architecture & Data Flow

```
main.py orchestrates per-user:
  1. get_active_users_with_resume()     → Supabase RPC call
  2. find_best_job_titles_for_user()    → LLM generates search titles
  3. get_jobs_for_user()                → scrapes 5 sites via jobspy
  4. clean_up_jobs()                    → dedup by URL + TF-IDF similarity, stop words, salary filter
  5. find_top_job_matches()             → TF-IDF cosine similarity: resume vs job descriptions → top 10
  6. get_job_ratings2()                 → LLM structured eval (16 yes/no questions → JobAssessment)
  7. add_derived_data()                 → LLM generates short_summary + hard_requirements
  8. save_jobs_to_supabase()            → upsert jobs + user-job associations
  9. find_existing_jobs_for_users()     → match recent DB jobs to users by title similarity
 10. send_email_updates()              → Mailjet HTML emails via Jinja2 templates
```

## Key Modules

| Module | Responsibility |
|---|---|
| `main.py` | Entry point, per-user orchestration, legacy `get_job_ratings()` (text-parsing scores) and current `get_job_ratings2()` (structured output scores) |
| `llm_config.py` | OpenRouter configuration: `get_openrouter_client()`, `MODEL_FAST`, `MODEL_STRUCTURED` constants |
| `llm.py` | Unified LLM interface via OpenRouter: `query_llm()`, `ask_chatgpt_about_job()`, `evaluate_job_match()` (structured output → `JobAssessment`) |
| `job_scraper.py` | Scraping via jobspy with exponential backoff, deduplication, derived data generation |
| `models.py` | `JobAssessment` Pydantic model — 16 boolean assessment fields + 3 text guidance fields |
| `calculate_scores.py` | Converts boolean assessments to weighted scores across 4 dimensions (desire, experience, requirements, experience_requirements), each 25% of overall |
| `analyzer.py` | TF-IDF vectorization for resume-to-job matching |
| `persistent_storage.py` | Supabase client (schema: `jobscraper`), CRUD for users/jobs/configs/associations |
| `job_helpers.py` | Job title generation, salary validation, stop word filtering, per-job guidance |
| `send_emails.py` | Mailjet integration, Jinja2 template rendering |
| `helpers.py` | Text normalization (`consolidate_text`) |
| `file_utils.py` | CSV export to ~/Downloads |

## Scoring System

The LLM evaluates each job with 16 yes/no questions (via `JobAssessment` Pydantic model with OpenAI structured output). These map to 4 weighted dimension scores:

- **Desire** (25%): title match (3), desired skills (3), no stop words (3), career step (2)
- **Experience** (25%): experience range (3), seniority (3), responsibilities (2), level (2)
- **Requirements** (25%): technical skills (3), domain skills (3), education (2), industry (2)
- **Experience Requirements** (25%): years required (3), role history (3), skill growth (2), environment (2)

Jobs scoring >50 overall are kept and saved.

## Database

- **Supabase** with schema `jobscraper`
- **Supabase MCP is available** — use `mcp__supabase__execute_sql` to query or modify the database directly, and `mcp__supabase__apply_migration` for DDL changes. Other MCP tools are available for logs, advisors, edge functions, branches, and type generation.
- Tables: `jobs`, `users`, `user_configs` (sparse key-value), `users_jobs` (junction with scores/guidance)
- View: `recent_high_score_jobs` (score >= 50, recent or marked interested/applied)
- RPC: `get_active_users_with_resume()`
- DDL scripts in `db_scripts/`

## Paired Web Frontend (`/Users/davidhague/source/job_scraper_web`)

Nuxt 3 (Vue 3 + TypeScript) app deployed to Vercel at https://jobs.timetovalue.org.

**How the two repos connect:** This scraper populates the `jobscraper` schema in Supabase (jobs, scores, derived data). The frontend reads from the same schema — primarily the `recent_high_score_jobs` view and `users_jobs` junction table. The GCP cloud function in `jobs-app-gcp/` is called by the frontend's `/api/onboarding` Nitro endpoint to generate initial jobs during user onboarding.

**Frontend key details:**
- **Dev server:** `npm run dev` — HTTPS on `localhost:3000` (requires local `localhost.pem` + `localhost-key.pem` certs for Google OAuth)
- **Build:** `npm run build`
- **Tests:** `npx playwright test` (E2E with Playwright/Chromium)
- **State:** Pinia store (`stores/jsaStore.ts`) holds `authUser`, `dbUser`, `currentJobs`, `selectedUserId`
- **DB access:** All queries go through `services/PersistentDataService.ts` (static methods wrapping Supabase client)
- **Auth:** Supabase Auth (email/password + Google OAuth)
- **API routes:** Nitro endpoints in `server/api/` proxy external services (OpenRouter LLM, Google Cloud Vision/Storage for resume OCR, Mailjet, GCP cloud function)
- **LLM:** Frontend uses Gemini Flash 1.5 via OpenRouter (not OpenAI like the scraper)
- **Components:** Vue 3 Composition API with `<script setup>`, no CSS framework (custom responsive CSS)
- **Pages:** `home.vue` (job dashboard with filters/sorting), `job/[id].vue` (detail view with score breakdown), `onboarding.vue` (4-step setup: resume → role → skills → about), `userprofile.vue` (settings)

**Shared Supabase schema (`jobscraper`):**
- Both repos use the same tables: `users`, `jobs`, `user_configs`, `users_jobs`
- Both repos use the same view: `recent_high_score_jobs`
- The scraper writes jobs/scores; the frontend reads them and writes user interactions (interested, applied)

## Code Style

Use PyCharm's built-in formatter with "Reformat code" on save.
