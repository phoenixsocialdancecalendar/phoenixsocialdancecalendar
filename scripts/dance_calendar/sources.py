from __future__ import annotations

import json
import re
from datetime import date, time

from dance_calendar.models import make_event, normalize_space
from dance_calendar.parsing import (
    DATE_PATTERN,
    WEEKDAY_MAP,
    deduplicate_events,
    event_from_jsonld,
    expand_monthly_occurrences,
    expand_weekly_occurrences,
    extract_jsonld_events,
    extract_links,
    extract_text_lines,
    infer_city,
    is_future_event,
    parse_date_label,
    parse_iso_datetime,
    parse_time_range,
    serialize_dt,
    strip_html,
)

SWING_DANCING_PHOENIX_API = "https://www.swingdancingphoenix.com/wp-json/tribe/events/v1/events"
SALSA_VIDA_CALENDAR_URL = "https://www.salsavida.com/guides/arizona/phoenix/calendar/"
PHXTMD_URL = "https://phxtmd.org/contra-dance"
GREATER_PHOENIX_SWING_URL = "https://greaterphoenixswingdanceclub.com/calendar"
PHOENIX_SALSA_DANCE_URL = "https://phoenixsalsadance.com/calendar/"
DAVE_AND_BUSTERS_TEMPE_URL = "https://www.daveandbusters.com/us/en/about/locations/tempe"


def fetch_swing_dancing_phoenix(fetch_text, today: date) -> list[dict[str, object]]:
    payload = json.loads(fetch_text(SWING_DANCING_PHOENIX_API))
    events: list[dict[str, object]] = []
    for item in payload.get("events", []):
        start_dt = parse_iso_datetime(item.get("start_date"))
        if not start_dt or start_dt.date() < today:
            continue
        end_dt = parse_iso_datetime(item.get("end_date"))
        venue_info = item.get("venue") or {}
        venue = normalize_space(str(venue_info.get("venue", "")))
        city = normalize_space(str(venue_info.get("city", "")))
        tags = ", ".join(tag.get("name", "") for tag in item.get("tags", []) if isinstance(tag, dict))
        notes = strip_html(str(item.get("description", "")))
        if len(notes) > 220:
            notes = notes[:217].rstrip() + "..."
        events.append(
            make_event(
                title=strip_html(str(item.get("title", ""))),
                start_at=serialize_dt(start_dt),
                end_at=serialize_dt(end_dt),
                venue=venue,
                city=city,
                dance_style=_infer_swing_style(tags, str(item.get("title", ""))),
                source_name="Swing Dancing Phoenix",
                source_url=str(item.get("url", "https://www.swingdancingphoenix.com/")),
                notes=notes,
            )
        )
    return events


def fetch_salsa_vida(fetch_text, today: date) -> list[dict[str, object]]:
    calendar_html = fetch_text(SALSA_VIDA_CALENDAR_URL)
    detail_links: list[str] = []
    for label, href in extract_links(calendar_html, base_url=SALSA_VIDA_CALENDAR_URL):
        if "/event/" not in href or "/guides/" in href or "phoenix" not in href.lower():
            continue
        if label.lower() in {"home", "contact", "events"}:
            continue
        if href not in detail_links:
            detail_links.append(href)

    events: list[dict[str, object]] = []
    for href in detail_links[:40]:
        detail_html = fetch_text(href)
        for payload in extract_jsonld_events(detail_html):
            event = event_from_jsonld(
                payload,
                source_name="Salsa Vida",
                source_url=href,
                default_style=_infer_salsa_style(str(payload.get("name", "")), str(payload.get("description", ""))),
            )
            if event and is_future_event(str(event["start_at"]), today):
                events.append(event)
    return deduplicate_events(events)


def fetch_phxtmd(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(PHXTMD_URL)
    lines = extract_text_lines(html, base_url=PHXTMD_URL)
    events: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        if not DATE_PATTERN.fullmatch(line):
            continue
        event_date = parse_date_label(line)
        if event_date is None or event_date < today:
            continue
        title = _nearest_title(lines, index)
        chunk = lines[index + 1 : index + 10]
        time_line = next((value for value in chunk if re.search(r"\d", value) and ("am" in value.lower() or "pm" in value.lower())), "")
        start_time, end_time = parse_time_range(time_line)
        if start_time is None:
            start_time = time(19, 0)
        venue_line = next((value for value in chunk if infer_city(value) or "AZ" in value), "")
        notes = " ".join(value for value in chunk if value not in {time_line, venue_line})
        start_dt = serialize_dt(parse_iso_datetime(f"{event_date.isoformat()}T{start_time.isoformat()}"))
        end_dt = serialize_dt(parse_iso_datetime(f"{event_date.isoformat()}T{(end_time or time(22, 0)).isoformat()}"))
        events.append(
            make_event(
                title=title or "Contra Dance",
                start_at=start_dt,
                end_at=end_dt,
                venue=venue_line,
                city=infer_city(venue_line, notes),
                dance_style="Contra",
                source_name="Phoenix Traditional Music and Dance Society",
                source_url=PHXTMD_URL,
                notes=notes,
            )
        )
    return deduplicate_events(events)


def fetch_greater_phoenix_swing(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(GREATER_PHOENIX_SWING_URL)
    lines = extract_text_lines(html, base_url=GREATER_PHOENIX_SWING_URL)
    joined = " ".join(lines)
    venue = next((line for line in lines if "Mesa" in line or "Phoenix" in line or "Scottsdale" in line), "")
    city = infer_city(venue, joined)

    friday_notes = _extract_line_containing(lines, "Friday Night Dance")
    sunday_notes = _extract_line_containing(lines, "First Sunday Swing Dance")

    events = []
    events.extend(
        expand_monthly_occurrences(
            title="Friday Night Dance",
            source_name="Greater Phoenix Swing Dance Club",
            source_url=GREATER_PHOENIX_SWING_URL,
            venue=venue,
            city=city,
            dance_style="Swing",
            notes=friday_notes or "First Friday swing dance.",
            occurrence=1,
            weekday=4,
            start_time=time(19, 15),
            end_time=time(22, 30),
            today=today,
        )
    )
    events.extend(
        expand_monthly_occurrences(
            title="First Sunday Swing Dance",
            source_name="Greater Phoenix Swing Dance Club",
            source_url=GREATER_PHOENIX_SWING_URL,
            venue=venue,
            city=city,
            dance_style="Swing",
            notes=sunday_notes or "First Sunday swing dance.",
            occurrence=1,
            weekday=6,
            start_time=time(17, 30),
            end_time=time(19, 45),
            today=today,
        )
    )
    return events


def fetch_phoenix_salsa_dance(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(PHOENIX_SALSA_DANCE_URL)
    lines = extract_text_lines(html, base_url=PHOENIX_SALSA_DANCE_URL)
    events: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        weekday = WEEKDAY_MAP.get(line.lower())
        if weekday is None:
            continue
        chunk_lines = lines[index + 1 : index + 7]
        chunk = " ".join(chunk_lines)
        time_match = re.search(r"\d{1,2}(?::\d{2})?\s*(?:am|pm).{0,12}\d{1,2}(?::\d{2})?\s*(?:am|pm)", chunk, re.IGNORECASE)
        title = next((value for value in chunk_lines if _is_probable_title_line(value)), "")
        if not title:
            continue
        start_time, end_time = parse_time_range(time_match.group(0) if time_match else "")
        if start_time is None:
            continue
        venue = next((value for value in chunk_lines if infer_city(value) or " at " in value.lower()), "")
        city = infer_city(venue, chunk)
        style = _infer_salsa_style(title, chunk)
        notes = " ".join(value for value in chunk_lines if value != title and value != venue)
        events.extend(
            expand_weekly_occurrences(
                title=title,
                source_name="Phoenix Salsa Dance",
                source_url=PHOENIX_SALSA_DANCE_URL,
                venue=venue,
                city=city,
                dance_style=style,
                notes=notes,
                weekday=weekday,
                start_time=start_time,
                end_time=end_time,
                today=today,
            )
        )
    return deduplicate_events(events)


def fetch_dave_and_busters_tempe(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(DAVE_AND_BUSTERS_TEMPE_URL)
    events: list[dict[str, object]] = []

    for payload in extract_jsonld_events(html):
        candidate = event_from_jsonld(
            payload,
            source_name="Dave & Buster's Tempe",
            source_url=DAVE_AND_BUSTERS_TEMPE_URL,
            default_style=_infer_dance_style(str(payload.get("name", "")), str(payload.get("description", ""))),
        )
        if candidate and _looks_like_dance_event(str(candidate["title"]), str(candidate["notes"])) and is_future_event(str(candidate["start_at"]), today):
            events.append(candidate)

    lines = extract_text_lines(html, base_url=DAVE_AND_BUSTERS_TEMPE_URL)
    for index, line in enumerate(lines):
        if not DATE_PATTERN.fullmatch(line):
            continue
        title = _nearest_title(lines, index)
        if not _looks_like_dance_event(title, " ".join(lines[max(0, index - 3) : index + 5])):
            continue
        event_date = parse_date_label(line)
        if event_date is None or event_date < today:
            continue
        chunk = lines[index + 1 : index + 8]
        time_line = next((value for value in chunk if "am" in value.lower() or "pm" in value.lower()), "")
        start_time, end_time = parse_time_range(time_line)
        if start_time is None:
            continue
        venue = "Dave & Buster's Tempe"
        notes = " ".join(chunk)
        events.append(
            make_event(
                title=title,
                start_at=serialize_dt(parse_iso_datetime(f"{event_date.isoformat()}T{start_time.isoformat()}")),
                end_at=serialize_dt(parse_iso_datetime(f"{event_date.isoformat()}T{(end_time or time(23, 0)).isoformat()}")),
                venue=venue,
                city="Tempe",
                dance_style=_infer_dance_style(title, notes),
                source_name="Dave & Buster's Tempe",
                source_url=DAVE_AND_BUSTERS_TEMPE_URL,
                notes=notes,
            )
        )

    return deduplicate_events(events)


def all_source_fetchers():
    return [
        fetch_swing_dancing_phoenix,
        fetch_salsa_vida,
        fetch_phxtmd,
        fetch_greater_phoenix_swing,
        fetch_phoenix_salsa_dance,
        fetch_dave_and_busters_tempe,
    ]


def _nearest_title(lines: list[str], index: int) -> str:
    ignored = {"Contra Dance", "Upcoming Events", "Event Details"}
    for candidate in reversed(lines[max(0, index - 4) : index]):
        if candidate in ignored or DATE_PATTERN.fullmatch(candidate):
            continue
        if len(candidate.split()) >= 2:
            return candidate
    return "Contra Dance"


def _extract_line_containing(lines: list[str], text: str) -> str:
    match = next((line for line in lines if text.lower() in line.lower()), "")
    return match


def _infer_swing_style(tags: str, title: str) -> str:
    haystack = f"{tags} {title}".lower()
    if "blues" in haystack:
        return "Swing / Blues"
    if "balboa" in haystack:
        return "Balboa"
    if "lindy" in haystack:
        return "Lindy Hop"
    return "Swing"


def _infer_salsa_style(title: str, notes: str) -> str:
    haystack = f"{title} {notes}".lower()
    if "bachata" in haystack and "salsa" in haystack:
        return "Salsa / Bachata"
    if "bachata" in haystack:
        return "Bachata"
    if "kizomba" in haystack:
        return "Kizomba"
    return "Salsa"


def _looks_like_dance_event(title: str, notes: str) -> bool:
    haystack = f"{title} {notes}".lower()
    keywords = [
        "dance",
        "salsa",
        "bachata",
        "swing",
        "contra",
        "line dance",
        "country dance",
        "ballroom",
        "latin night",
        "social dance",
    ]
    blockers = ["dance games", "arcade", "power card", "improve dance style"]
    return any(keyword in haystack for keyword in keywords) and not any(blocker in haystack for blocker in blockers)


def _infer_dance_style(title: str, notes: str) -> str:
    haystack = f"{title} {notes}".lower()
    if "salsa" in haystack and "bachata" in haystack:
        return "Salsa / Bachata"
    if "bachata" in haystack:
        return "Bachata"
    if "contra" in haystack:
        return "Contra"
    if "swing" in haystack:
        return "Swing"
    if "line dance" in haystack or "country" in haystack:
        return "Country"
    if "ballroom" in haystack:
        return "Ballroom"
    return "Social Dance"


def _is_probable_title_line(value: str) -> bool:
    cleaned = value.strip()
    lower = cleaned.lower()
    if not cleaned or len(cleaned) < 4:
        return False
    if cleaned.startswith("•") or cleaned.startswith("-"):
        return False
    if "am" in lower or "pm" in lower:
        return False
    if lower.startswith("price:") or "call for more info" in lower:
        return False
    if infer_city(cleaned) or lower.startswith("http"):
        return False
    return _looks_like_dance_event(cleaned, "")
