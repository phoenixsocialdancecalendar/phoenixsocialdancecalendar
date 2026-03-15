from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dance_calendar.models import is_cancelled_event, make_event, normalize_space
from dance_calendar.parsing import deduplicate_events, is_future_event
from dance_calendar.sources import SourceDefinition, all_sources

USER_AGENT = "PhoenixDanceCalendarBot/1.0 (+https://example.com)"
FETCH_TIMEOUT_SECONDS = 30
FETCH_MAX_ATTEMPTS = 3
FETCH_RETRY_DELAYS_SECONDS = (1, 2)
WARNING_STATUS = "warning"
ERROR_STATUS = "error"
OK_STATUS = "ok"


def fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urlopen(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
                quality_flags=["manual_source", *list(item.get("quality_flags") or [])],
                quality_note=str(item.get("quality_note", "")) or None,
            )
        )
    return events


def build_event_catalog(*, today: date | None = None, manual_path: Path | None = None) -> list[dict[str, object]]:
    events, _report = build_event_catalog_with_report(today=today, manual_path=manual_path)
    return events


def build_event_catalog_with_report(
    *,
    today: date | None = None,
    manual_path: Path | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    today = today or date.today()
    manual_path = manual_path or Path("_data/manual_events.json")
    manual_events = [event for event in load_manual_events(manual_path) if not is_cancelled_event(event)]
    source_runs = [_build_manual_source_run(manual_events, manual_path)]
    events = list(manual_events)

    for source in all_sources():
        source_run = run_source(source, today=today)
        source_runs.append(source_run)
        events.extend(source_run["raw_events"])

    events = [event for event in events if is_future_event(str(event["start_at"]), today)]
    events = suppress_cdc_ocr_duplicates(events)
    events = deduplicate_events(events)
    events.sort(key=lambda event: (str(event["start_at"]), str(event["title"])))
    report = build_source_health_report(source_runs, events, today=today, manual_path=manual_path)
    return events, report


def run_source(source: SourceDefinition, *, today: date) -> dict[str, object]:
    started_at = iso_now()
    fetch_count = 0
    retry_count = 0

    def fetch_with_retries(url: str) -> str:
        nonlocal fetch_count, retry_count
        fetch_count += 1
        last_error: Exception | None = None
        for attempt in range(1, FETCH_MAX_ATTEMPTS + 1):
            try:
                return fetch_text(url)
            except (HTTPError, TimeoutError, URLError, OSError) as exc:
                last_error = exc
                if attempt >= FETCH_MAX_ATTEMPTS:
                    raise
                retry_count += 1
                sleep(FETCH_RETRY_DELAYS_SECONDS[min(attempt - 1, len(FETCH_RETRY_DELAYS_SECONDS) - 1)])
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Failed to fetch {url}")

    warnings: list[str] = []
    errors: list[str] = []
    events: list[dict[str, object]] = []
    try:
        events = source.fetcher(fetch_with_retries, today)
        events = [event for event in events if not is_cancelled_event(event)]
    except Exception as exc:
        errors.append(f"{type(exc).__name__}: {exc}")
        status = ERROR_STATUS
    else:
        suspicious_count = sum(1 for event in events if has_quality_note(event))
        if suspicious_count:
            warnings.append(f"{suspicious_count} event(s) flagged for low-confidence details.")
        if retry_count:
            warnings.append(f"Retried {retry_count} request(s) before succeeding.")
        status = WARNING_STATUS if warnings else OK_STATUS

    finished_at = iso_now()
    return {
        "source_name": source.name,
        "source_url": source.url,
        "status": status,
        "events": [summarize_event(event) for event in events],
        "warnings": warnings,
        "errors": errors,
        "counts": {
            "events": len(events),
            "suspicious_events": sum(1 for event in events if has_quality_note(event)),
            "fetches": fetch_count,
            "retries": retry_count,
        },
        "started_at": started_at,
        "finished_at": finished_at,
        "raw_events": events,
    }


def _build_manual_source_run(events: list[dict[str, object]], manual_path: Path) -> dict[str, object]:
    started_at = iso_now()
    finished_at = iso_now()
    suspicious_count = sum(1 for event in events if has_quality_note(event))
    warnings = [f"{suspicious_count} manual event(s) flagged for low-confidence details."] if suspicious_count else []
    return {
        "source_name": "Manual Entries",
        "source_url": str(manual_path),
        "status": WARNING_STATUS if warnings else OK_STATUS,
        "events": [summarize_event(event) for event in events],
        "warnings": warnings,
        "errors": [],
        "counts": {
            "events": len(events),
            "suspicious_events": suspicious_count,
            "fetches": 0,
            "retries": 0,
        },
        "started_at": started_at,
        "finished_at": finished_at,
        "raw_events": list(events),
    }


def build_source_health_report(
    source_runs: list[dict[str, object]],
    events: list[dict[str, object]],
    *,
    today: date,
    manual_path: Path,
) -> dict[str, object]:
    visible_source_runs = [strip_raw_events(source_run) for source_run in source_runs]
    return {
        "generated_at": iso_now(),
        "today": today.isoformat(),
        "manual_path": str(manual_path),
        "summary": {
            "sources_total": len(visible_source_runs),
            "sources_ok": sum(1 for source_run in visible_source_runs if source_run["status"] == OK_STATUS),
            "sources_warning": sum(1 for source_run in visible_source_runs if source_run["status"] == WARNING_STATUS),
            "sources_error": sum(1 for source_run in visible_source_runs if source_run["status"] == ERROR_STATUS),
            "events_total": len(events),
            "events_with_warnings": sum(1 for event in events if has_quality_note(event)),
        },
        "sources": visible_source_runs,
    }


def summarize_event(event: dict[str, object]) -> dict[str, object]:
    return {
        "id": event.get("id"),
        "title": event.get("title"),
        "start_at": event.get("start_at"),
        "quality_flags": list(event.get("quality_flags") or []),
        "quality_note": event.get("quality_note"),
    }


def has_quality_note(event: dict[str, object]) -> bool:
    quality_note = event.get("quality_note")
    return isinstance(quality_note, str) and bool(normalize_space(quality_note))


def strip_raw_events(source_run: dict[str, object]) -> dict[str, object]:
    visible = dict(source_run)
    visible.pop("raw_events", None)
    return visible


def suppress_cdc_ocr_duplicates(events: list[dict[str, object]]) -> list[dict[str, object]]:
    covered_dates = {
        str(event["start_at"])[:10]
        for event in events
        if str(event.get("source_name", "")) == "White Rabbit WCS"
        and normalize_space(str(event.get("venue", ""))).lower() == "creative dance collective"
        and normalize_space(str(event.get("city", ""))).lower() == "mesa"
        and str(event.get("dance_style", "")) == "West Coast Swing"
    }
    if not covered_dates:
        return events

    return [
        event
        for event in events
        if not (
            str(event.get("source_name", "")) == "CDC Studios"
            and normalize_space(str(event.get("venue", ""))).lower() == "creative dance collective"
            and normalize_space(str(event.get("city", ""))).lower() == "mesa"
            and str(event.get("dance_style", "")) == "West Coast Swing"
            and str(event["start_at"])[:10] in covered_dates
        )
    ]


def write_events(events: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(events, indent=2, sort_keys=True, ensure_ascii=False) + "\n")


def write_report(report: dict[str, object], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
