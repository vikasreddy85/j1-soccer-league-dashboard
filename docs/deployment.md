# Deploying for Free

[Streamlit Community Cloud](https://share.streamlit.io) is the easiest free
way to host this dashboard — no server management, deploys straight from
GitHub, and auto-redeploys on push.

## 1. Push to GitHub

Public repos are free on Community Cloud (private repos need a paid plan).
Make sure the repo includes:

- `dashboard.py`, `train_model.py`, `fan_simulator.py`, `db_setup.py`
- `requirements.txt`
- **The data files**: `j1_league.duckdb` and `outcome_predictor.joblib`

The deployed container starts from a clean checkout of your repo — it
doesn't have access to your local filesystem, so the DuckDB file and the
trained model need to actually be in (or fetched by) the repo, not just
present on your laptop.

!!! tip "Large files"
    GitHub has a 100 MB per-file limit and gets unhappy well before that.
    If `j1_league.duckdb` or the `.joblib` model are large, use
    [Git LFS](https://git-lfs.com/), or host them in cheap/free object
    storage (e.g. a public bucket) and download them at startup inside
    `load_model()` / `load_matches()` if they're not already present
    on disk.

## 2. Deploy

1. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub.
2. Click **New app**, pick the repo, branch, and `dashboard.py` as the entry point.
3. Deploy. It builds in a couple of minutes and gives you a public URL.
4. Future pushes to the branch you deployed from redeploy automatically.

## 3. Fan reactions won't work out of the box — and that's fine

`fan_simulator.py` talks to `http://localhost:11434` (a local Ollama
instance). Streamlit Cloud's container has no Ollama and can't reach your
machine, so that feature is unreachable once deployed.

The dashboard handles this automatically: it calls
`fan_simulator.check_ollama_available()` before attempting anything, and
shows a short explanatory message instead of a broken widget if Ollama
can't be reached. Everything else — predictions, backtest accuracy, the
matchup picker — works identically to running locally.

See [Fan Reactions](fan-reactions.md) if you'd like to swap in a hosted LLM
API instead, so that feature works on the deployed version too.

## 4. Secrets (if you add a hosted LLM API later)

Don't commit API keys. Use Community Cloud's **Settings → Secrets** panel
(or a local `.streamlit/secrets.toml`, which should be gitignored) and read
them in code with `st.secrets["YOUR_KEY"]`.

## Other free options

If you outgrow Community Cloud's resource limits (limited CPU/memory,
session timeouts, public-repo requirement) or want more control:

- **Hugging Face Spaces** — free tier, supports Streamlit directly, works with private Spaces too.
- **Render / Railway free tiers** — more general-purpose, run any container, but free tiers tend to sleep after inactivity and have tighter monthly hour limits.

All of the same caveats apply: the DuckDB file and trained model need to
ship with (or be fetched by) the deployment, and anything talking to
`localhost` won't reach your machine once it's running somewhere else.