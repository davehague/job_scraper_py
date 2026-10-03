"""Runtime switches read from environment variables.

Kept dependency-free so it can be imported (and unit-tested) without touching
Supabase, Mailjet, or the LLM clients.
"""
import os

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def env_flag(name: str, default: bool) -> bool:
    """Read a boolean flag from the environment.

    Accepts true/false/1/0/yes/no/on/off (case-insensitive). Unset or
    unrecognised values fall back to ``default``.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return default


def emails_enabled() -> bool:
    """Whether the run may send user emails via Mailjet.

    Controlled by SEND_EMAILS. Defaults to True so existing setups are
    unchanged; set SEND_EMAILS=false to run the full pipeline (scrape, score,
    save) without emailing anyone.
    """
    return env_flag("SEND_EMAILS", default=True)


def log_to_file() -> bool:
    """Whether main.py should redirect stdout/stderr to ~/Downloads/job_scraper_<date>.log.

    Controlled by LOG_TO_FILE. Defaults to True (the historical "SCHEDULED"
    behaviour). Set LOG_TO_FILE=false when a supervisor such as systemd or a
    job wrapper already captures stdout/stderr.
    """
    return env_flag("LOG_TO_FILE", default=True)
