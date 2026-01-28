import os

from openai import OpenAI

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
MODEL_FAST = os.environ.get("LLM_MODEL_FAST", "openai/gpt-4.1-nano")
MODEL_STRUCTURED = os.environ.get("LLM_MODEL_STRUCTURED", "openai/gpt-5-mini")
APP_SITE_URL = "https://jobs.timetovalue.org"
APP_TITLE = "Job Scraper"


def get_openrouter_client() -> OpenAI:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("Environment variable OPENROUTER_API_KEY is not set.")
    return OpenAI(
        base_url=OPENROUTER_BASE_URL,
        api_key=api_key,
        default_headers={
            "HTTP-Referer": APP_SITE_URL,
            "X-Title": APP_TITLE,
        },
    )
