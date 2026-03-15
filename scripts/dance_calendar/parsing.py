from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timedelta
from html import unescape
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from dance_calendar.models import make_event, normalize_space

PHOENIX_TZ = ZoneInfo("America/Phoenix")
METRO_CITIES = [
    "Apache Junction",
    "Chandler",
    "Gilbert",
    "Glendale",
    "Goodyear",
    "Mesa",
    "Peoria",
    "Phoenix",
    "Queen Creek",
    "Scottsdale",
    "Surprise",
    "Tempe",
  ]

BLOCK_TAGS = {
    "article",
    "br",
    "div",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "li",
    "main",
    "p",
    "section",
    "tr",
    "ul",
}

JSONLD_PATTERN = re.compile(
    r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
MONTH_PATTERN = r"(January|February|March|April|May|June|July|August|September|October|November|December)"
DATE_PATTERN = re.compile(rf"{MONTH_PATTERN}\s+\d{{1,2}},\s+\d{{4}}")
TIME_RANGE_PATTERN = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*(?:-|to|–)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
    re.IGNORECASE,
)
TIME_VALUE_PATTERN = re.compile(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)", re.IGNORECASE)
WEEKDAY_MAP = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


class TextAndLinkExtractor(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.lines: list[str] = []
        self.current: list[str] = []
        self.links: list[tuple[str, str]] = []
        self.anchor_href: str | None = None
        self.anchor_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in BLOCK_TAGS:
            self.flush()
        if tag == "a":
            self.anchor_href = dict(attrs).get("href")
            self.anchor_parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag in BLOCK_TAGS:
            self.flush()
        if tag == "a":
            label = normalize_space(" ".join(self.anchor_parts))
            href = self.anchor_href
            if href:
                self.links.append((label, urljoin(self.base_url, href)))
            self.anchor_href = None
            self.anchor_parts = []

    def handle_data(self, data: str) -> None:
        text = normalize_space(unescape(data))
        if not text:
            return
        self.current.append(text)
        if self.anchor_href is not None:
            self.anchor_parts.append(text)

    def flush(self) -> None:
        line = normalize_space(" ".join(self.current))
        if line:
            self.lines.append(line)
        self.current = []


def extract_text_lines(html: str, *, base_url: str = "") -> list[str]:
    parser = TextAndLinkExtractor(base_url)
    parser.feed(html)
    parser.flush()
    return parser.lines


def extract_links(html: str, *, base_url: str) -> list[tuple[str, str]]:
    parser = TextAndLinkExtractor(base_url)
    parser.feed(html)
    return parser.links


def strip_html(html: str) -> str:
    return " ".join(extract_text_lines(html))


def extract_jsonld_events(html: str) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for match in JSONLD_PATTERN.finditer(html):
        raw_json = match.group(1).strip()
        if not raw_json:
            continue
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        events.extend(_collect_jsonld_events(payload))
    return events


def _collect_jsonld_events(payload: object) -> list[dict[str, object]]:
    collected: list[dict[str, object]] = []
    if isinstance(payload, dict):
        type_value = payload.get("@type")
        types = type_value if isinstance(type_value, list) else [type_value]
        if any(value == "Event" for value in types if isinstance(value, str)):
            collected.append(payload)
        for value in payload.values():
            collected.extend(_collect_jsonld_events(value))
    elif isinstance(payload, list):
        for value in payload:
            collected.extend(_collect_jsonld_events(value))
    return collected


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=PHOENIX_TZ)
    return parsed.astimezone(PHOENIX_TZ)


def combine_local(target_date: date, target_time: time) -> datetime:
    return datetime.combine(target_date, target_time, tzinfo=PHOENIX_TZ)


def serialize_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(PHOENIX_TZ).isoformat()


def parse_date_label(value: str) -> date | None:
    cleaned = normalize_space(re.sub(r"^[A-Za-z]+,\s*", "", value))
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def parse_time_range(value: str) -> tuple[time, time | None] | tuple[None, None]:
    match = TIME_RANGE_PATTERN.search(value)
    if match:
        start = _build_time(match.group(1), match.group(2), match.group(3))
        end = _build_time(match.group(4), match.group(5), match.group(6))
        return start, end
    match = TIME_VALUE_PATTERN.search(value)
    if match:
        return _build_time(match.group(1), match.group(2), match.group(3)), None
    return None, None


def _build_time(hour_text: str, minute_text: str | None, meridiem: str) -> time:
    hour = int(hour_text)
    minute = int(minute_text or "0")
    meridiem = meridiem.lower()
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return time(hour=hour, minute=minute)


def infer_city(*values: str) -> str:
    haystack = " ".join(values)
    matches: list[tuple[int, str]] = []
    for city in METRO_CITIES:
        match = re.search(rf"\b{re.escape(city)}\b", haystack, re.IGNORECASE)
        if match:
            matches.append((match.start(), city))
    if not matches:
        return ""
    matches.sort(key=lambda item: item[0])
    return matches[0][1]


def is_future_event(start_at: str, today: date) -> bool:
    start_date = parse_iso_datetime(start_at)
    return bool(start_date and start_date.date() >= today)


def nth_weekday_of_month(year: int, month: int, weekday: int, occurrence: int) -> date:
    first_day = date(year, month, 1)
    offset = (weekday - first_day.weekday()) % 7
    return first_day + timedelta(days=offset + ((occurrence - 1) * 7))


def expand_monthly_occurrences(
    *,
    title: str,
    source_name: str,
    source_url: str,
    venue: str,
    city: str,
    dance_style: str,
    notes: str,
    occurrence: int,
    weekday: int,
    start_time: time,
    end_time: time | None,
    today: date,
    months_ahead: int = 4,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for step in range(months_ahead):
        year = today.year + ((today.month - 1 + step) // 12)
        month = ((today.month - 1 + step) % 12) + 1
        start_date = nth_weekday_of_month(year, month, weekday, occurrence)
        if start_date < today:
            continue
        start_dt = combine_local(start_date, start_time)
        end_dt = combine_local(start_date, end_time) if end_time else None
        events.append(
            make_event(
                title=title,
                start_at=serialize_dt(start_dt),
                end_at=serialize_dt(end_dt),
                venue=venue,
                city=city,
                dance_style=dance_style,
                source_name=source_name,
                source_url=source_url,
                notes=notes,
            )
        )
    return events


def expand_weekly_occurrences(
    *,
    title: str,
    source_name: str,
    source_url: str,
    venue: str,
    city: str,
    dance_style: str,
    notes: str,
    weekday: int,
    start_time: time,
    end_time: time | None,
    today: date,
    weeks_ahead: int = 8,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    offset = (weekday - today.weekday()) % 7
    first_date = today + timedelta(days=offset)
    for step in range(weeks_ahead):
        start_date = first_date + timedelta(days=step * 7)
        start_dt = combine_local(start_date, start_time)
        end_dt = combine_local(start_date, end_time) if end_time else None
        events.append(
            make_event(
                title=title,
                start_at=serialize_dt(start_dt),
                end_at=serialize_dt(end_dt),
                venue=venue,
                city=city,
                dance_style=dance_style,
                source_name=source_name,
                source_url=source_url,
                notes=notes,
            )
        )
    return events


def event_from_jsonld(
    payload: dict[str, object],
    *,
    source_name: str,
    source_url: str,
    default_style: str,
) -> dict[str, object] | None:
    title = normalize_space(str(payload.get("name", "")))
    start_dt = parse_iso_datetime(payload.get("startDate"))  # type: ignore[arg-type]
    if not title or not start_dt:
        return None
    end_dt = parse_iso_datetime(payload.get("endDate"))  # type: ignore[arg-type]
    location = payload.get("location")
    venue = ""
    city = ""
    if isinstance(location, dict):
        venue = normalize_space(str(location.get("name", "")))
        address = location.get("address")
        if isinstance(address, dict):
            city = normalize_space(str(address.get("addressLocality", "")))
            venue = venue or normalize_space(str(address.get("streetAddress", "")))
        elif isinstance(address, str):
            city = infer_city(address)
    description = normalize_space(strip_html(str(payload.get("description", ""))))
    if len(description) > 240:
        description = description[:237].rstrip() + "..."
    return make_event(
        title=title,
        start_at=serialize_dt(start_dt),
        end_at=serialize_dt(end_dt),
        venue=venue,
        city=city,
        dance_style=default_style,
        source_name=source_name,
        source_url=str(payload.get("url") or source_url),
        notes=description,
    )


def normalize_for_match(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", normalize_space(value).lower()).strip()


def token_similarity(left: str, right: str) -> float:
    left_tokens = set(normalize_for_match(left).split())
    right_tokens = set(normalize_for_match(right).split())
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    return overlap / max(len(left_tokens), len(right_tokens))


def merge_notes(primary: str, secondary: str) -> str:
    notes = [value for value in [primary, secondary] if normalize_space(value)]
    unique: list[str] = []
    for note in notes:
        if note not in unique:
            unique.append(note)
    return " ".join(unique)


def deduplicate_events(events: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    deduped: list[dict[str, object]] = []
    for candidate in sorted(events, key=lambda event: (str(event["start_at"]), str(event["title"]))):
        match = next((existing for existing in deduped if _likely_same_event(existing, candidate)), None)
        if match is None:
            deduped.append(candidate)
            continue
        match["venue"] = match["venue"] or candidate["venue"]
        match["city"] = match["city"] or candidate["city"]
        match["notes"] = merge_notes(str(match["notes"]), str(candidate["notes"]))
        if match["source_name"] != candidate["source_name"]:
            match["notes"] = merge_notes(
                str(match["notes"]),
                f"Also listed on {candidate['source_name']}.",
            )
    return deduped


def _likely_same_event(left: dict[str, object], right: dict[str, object]) -> bool:
    if left["start_at"] != right["start_at"]:
        return False
    left_title = normalize_for_match(str(left["title"]))
    right_title = normalize_for_match(str(right["title"]))
    title_close = (
        token_similarity(left_title, right_title) >= 0.75
        or left_title in right_title
        or right_title in left_title
    )
    place_close = token_similarity(str(left["venue"]), str(right["venue"])) >= 0.75 or (
        normalize_for_match(str(left["city"])) and normalize_for_match(str(left["city"])) == normalize_for_match(str(right["city"]))
    )
    return title_close and place_close
