"""Load user-owned webhook destinations without embedding deployment credentials."""
import json
import os
import re
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True)
class Destination:
    name: str
    url: str
    token: str
    receipt: str = "http"


def load_destinations():
    # An explicit list replaces legacy Grok Bot settings; it is never added twice.
    raw = os.getenv("DESTINATIONS_JSON", "").strip()
    if raw:
        entries = json.loads(raw)
        if not isinstance(entries, list) or not entries:
            raise RuntimeError("DESTINATIONS_JSON must be a non-empty list")
    else:
        entries = [{"name": "grokbot", "url": os.getenv("GROKBOT_WEBHOOK_URL", ""), "token": os.getenv("GROKBOT_WEBHOOK_KEY", "")}]
    destinations = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) - {"name", "url", "token", "receipt"}:
            raise RuntimeError("Invalid destination fields")
        name, url, token = (entry.get(field) for field in ("name", "url", "token"))
        receipt = entry.get("receipt", "http")
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", name):
            raise RuntimeError("Destination name must be a short lowercase identifier")
        if not isinstance(url, str) or not isinstance(token, str) or not token:
            raise RuntimeError("Destination requires an HTTPS URL and token")
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
            raise RuntimeError("Destination URL must use HTTPS without embedded credentials or fragment")
        if receipt not in ("http", "completed"):
            raise RuntimeError("Destination receipt must be http or completed")
        if any(item.name == name for item in destinations):
            raise RuntimeError("Destination names must be unique")
        destinations.append(Destination(name, url, token, receipt))
    return destinations
