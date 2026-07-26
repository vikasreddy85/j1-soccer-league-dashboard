import requests
import json

OLLAMA_URL = "http://localhost:11434/api/chat"
MODEL_NAME = "gemma3:4b"

PERSONAS = [
    {
        "id": "home_ultra",
        "label": "Home ultras",
        "instructions": "You are a die-hard supporter of the HOME team. You are passionate, "
                         "a little biased, and confident. Keep it to one or two sentences, "
                         "written like a real social media post — casual, no hashtags."
    },
    {
        "id": "away_fan",
        "label": "Away supporters",
        "instructions": "You are a supporter of the AWAY team. You're realistic about your "
                         "team's chances but defensive when others doubt them. One or two "
                         "sentences, casual social media tone, no hashtags."
    },
    {
        "id": "neutral_pundit",
        "label": "Neutral pundit",
        "instructions": "You are a neutral football analyst commenting on this matchup. "
                         "Be measured and reference the statistical context given. One or "
                         "two sentences, no hashtags."
    },
]


def check_ollama_available(timeout: float = 2.0) -> bool:
    try:
        resp = requests.get(OLLAMA_URL.replace("/api/chat", "/api/tags"), timeout=timeout)
        return resp.ok
    except requests.exceptions.RequestException:
        return False


def _call_ollama(system_prompt: str, user_prompt: str) -> str:
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "stream": False,
    }
    try:
        resp = requests.post(OLLAMA_URL, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("message", {}).get("content", "").strip()
    except requests.exceptions.RequestException as e:
        return f"[Ollama unreachable — is it running? ({e})]"


def simulate_reactions(home_team: str, away_team: str, home_win_prob: float,
                        draw_prob: float, away_win_prob: float) -> list:
    match_context = (
        f"Upcoming match: {home_team} (home) vs {away_team} (away).\n"
        f"Model win probabilities — {home_team}: {home_win_prob:.0%}, "
        f"Draw: {draw_prob:.0%}, {away_team}: {away_win_prob:.0%}."
    )

    results = []
    for persona in PERSONAS:
        text = _call_ollama(persona["instructions"], match_context)
        results.append({
            "persona": persona["id"],
            "label": persona["label"],
            "text": text,
        })
    return results


if __name__ == "__main__":
    reactions = simulate_reactions("Kashima Antlers", "Urawa Red Diamonds", 0.45, 0.28, 0.27)
    for r in reactions:
        print(f"[{r['label']}] {r['text']}")