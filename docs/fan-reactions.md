# Fan Reactions (Ollama)

`fan_simulator.py` asks a local LLM to write short, in-character reactions
to a predicted matchup from three personas:

| Persona | Behavior |
|---|---|
| Home ultras | Passionate, biased, confident |
| Away supporters | Realistic but defensive |
| Neutral pundit | Measured, references the stated probabilities |

This talks to **Ollama running on your own machine** — `http://localhost:11434`
by default — not a hosted API. That's a deliberate choice (no API key, no
per-call cost), but it means the feature only works where Ollama is actually
running.

## Setup

```bash
ollama pull llama3.1     # or any model — update MODEL_NAME in fan_simulator.py
ollama serve
```

`MODEL_NAME` in `fan_simulator.py` defaults to `gemma3:4b`; change it to
whatever model you've pulled.

## Automatic graceful degradation

`fan_simulator.py` exposes `check_ollama_available()`, a fast health check
against Ollama's `/api/tags` endpoint. The dashboard calls this before
attempting simulation:

```python
if check_ollama_available():
    reactions = simulate_reactions(...)
    ...
else:
    st.info("Fan reaction simulation is unavailable right now...")
```

This matters most when the dashboard is deployed somewhere like Streamlit
Community Cloud, which has no local Ollama instance to reach — without the
check, every prediction would sit through a spinner only to show a wall of
`[Ollama unreachable]` errors, one per persona. With it, the section is
simply replaced with a short explanation, and the rest of the dashboard
(predictions, backtest, charts) works exactly as normal.

If you want fan reactions to work on a hosted deployment too, swap
`_call_ollama()` in `fan_simulator.py` for a call to a hosted LLM API
(Anthropic, OpenAI, etc.), and store the API key in your hosting platform's
secrets manager rather than in code.