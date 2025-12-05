# Repository Guidelines

## Project Structure & Module Organization
Streamlit logic, data assembly, and page layout live in `reforma-trib-app-tabs.py`; each tab reads SPED Contribuições or Fiscal uploads, builds normalized IDs, and caches results via `@st.cache_data`. Shared code tables (CSTs, regimes, etc.) are centralized in `dicts.py`—extend those dictionaries here rather than sprinkling literals through the UI. Sample SPED/ECD payloads for manual verification sit in `arquivos_teste/`, while `.streamlit/config.toml` carries server limits (1 GB upload/message). Keep temporary notebooks or exports out of the repo root to avoid confusing the app runner.

## Build, Test, and Development Commands
- `python -m venv .venv && source .venv/bin/activate`: Create and activate a local virtual environment.
- `pip install -r requirements.txt`: Install Streamlit, pandas, numpy, and matplotlib dependencies.
- `streamlit run reforma-trib-app-tabs.py`: Launch TaxDash locally at `http://localhost:8501`.
- `streamlit run reforma-trib-app-tabs.py --server.headless true`: Useful for CI smoke checks or when running over SSH.
Stop the server with `Ctrl+C` so cached artifacts clear cleanly between runs.

## Coding Style & Naming Conventions
Stick to Python 3.11+ with 4-space indentation and PEP 8 spacing. Prefer descriptive snake_case for variables (e.g., `df_temp`, `prefix_periodo`) and UPPER_SNAKE for constants and dictionary names. Keep dataframe transformations vectorized; avoid per-row loops when pandas can broadcast. Guard user-visible strings in `dicts.py` or a dedicated constants block, and rely on Streamlit’s status elements (`st.error`, `st.stop`) for flow control rather than bare exceptions.

## Testing Guidelines
No automated suite yet; validate changes interactively. Use fixtures under `arquivos_teste/` to simulate multi-file uploads and confirm IDs/caches behave as expected. When altering parsing logic, log the first few filtered rows with `st.dataframe` inside an `st.expander` so reviewers can inspect raw output. Before publishing, run `streamlit run ... --server.headless true` to catch import errors, and document any new caching requirements in the PR.

## Commit & Pull Request Guidelines
History favors concise, present-tense summaries (e.g., `improved sped loading process`). Follow the same style: lowercase subject line under 60 chars that describes what changed, not how. Each pull request should include: a one-paragraph change description, testing notes (“Uploaded 2× SPED Fiscal samples”), any relevant screenshots/GIFs of new UI, and references to tracked issues. Keep branches focused; large refactors plus feature work belong in separate PRs.
