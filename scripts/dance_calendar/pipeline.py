from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from urllib.request import Request, urlopen

from dance_calendar.models import make_event
from dance_calendar.parsing import deduplicate_events, is_future_event
from dance_calendar.sources import all_source_fetchers

USER_AGENT = "PhoenixDanceCalendarBot/1.0 (+https://example.com)"


def fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urlopen(request, timeout=30) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def load_manual_events(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text())
    events: list[dict[str, object]] = []
    for item in payload:
        if not item.get("start_at"):
            continue
        events.append(
            make_event(
                title=str(item.get("title", "")),
                start_at=str(item["start_at"]),
                end_at=str(item.get("end_at")) if item.get("end_at") else None,
                venue=str(item.get("venue", "")),
                city=str(item.get("city", "")),
                dance_style=str(item.get("dance_style", "Social Dance")),
                source_name=str(item.get("source_name", "Manual Entry")),
                source_url=str(item.get("source_url", "")),
                notes=str(item.get("notes", "")),
                activity_kind=str(item.get("activity_kind", "")) or None,
                all_day=bool(item.get("all_day", False)),
                last_seen_at=str(item.get("last_seen_at", "")) or None,
                event_id=str(item.get("id", "")) or None,
            )
        )
    return events


def build_event_catalog(*, today: date | None = None, manual_path: Path | None = None) -> list[dict[str, object]]:
    today = today or date.today()
    manual_path = manual_path or Path("_data/manual_events.json")
    events = load_manual_events(manual_path)

    for fetcher in all_source_fetchers():
        try:
            events.extend(fetcher(fetch_text, today))
        except Exception as exc:
            print(f"[warn] {fetcher.__name__} failed: {exc}")

    events = [event for event in events if is_future_event(str(event["start_at"]), today)]
    events = deduplicate_events(events)
    events.sort(key=lambda event: (str(event["start_at"]), str(event["title"])))
    return events


def write_events(events: list[dict[str, object]], output_path: Path) -> None:
    output_path.write_text(json.dumps(events, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
