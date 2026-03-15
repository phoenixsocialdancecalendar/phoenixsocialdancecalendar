from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timedelta
from html import unescape
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from dance_calendar.models import make_event, normalize_quality_flags, normalize_space, quality_note_for_flags

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
IGNORED_CONTENT_TAGS = {
    "noscript",
    "script",
    "style",
    "template",
}

JSONLD_PATTERN = re.compile(
    r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
HTML_ESCAPE_PATTERN = re.compile(r"&(?:[a-zA-Z]+|#\d+|#x[a-fA-F0-9]+);|\\[nr,:;]")
SCRIPT_NOISE_PATTERNS = [
    re.compile(r"window\.__NUXT__.*", re.IGNORECASE),
    re.compile(r"__NEXT_DATA__.*", re.IGNORECASE),
]
MONTH_PATTERN = r"(January|February|March|April|May|June|July|August|September|October|November|December)"
DATE_PATTERN = re.compile(rf"{MONTH_PATTERN}\s+\d{{1,2}},\s+\d{{4}}")
TIME_RANGE_PATTERN = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*(?:-|to|–)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
    re.IGNORECASE,
)
PARTIAL_TIME_RANGE_PATTERN = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(?:-|to|–)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?",
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
ICS_WEEKDAY_MAP = {
    "MO": 0,
    "TU": 1,
    "WE": 2,
    "TH": 3,
    "FR": 4,
    "SA": 5,
    "SU": 6,
}


class TextAndLinkExtractor(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.ignored_depth = 0
        self.lines: list[str] = []
        self.current: list[str] = []
        self.links: list[tuple[str, str]] = []
        self.anchor_href: str | None = None
        self.anchor_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in IGNORED_CONTENT_TAGS:
            self.flush()
            self.ignored_depth += 1
            return
        if self.ignored_depth:
            return
        if tag in BLOCK_TAGS:
            self.flush()
        if tag == "a":
            self.anchor_href = dict(attrs).get("href")
            self.anchor_parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag in IGNORED_CONTENT_TAGS:
            self.ignored_depth = max(0, self.ignored_depth - 1)
            return
        if self.ignored_depth:
            return
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
        if self.ignored_depth:
            return
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


def decode_text(value: str | None, *, strip_markup: bool = False) -> str:
    text = value or ""
    text = (
        text.replace("\\n", " ")
        .replace("\\r", " ")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\:", ":")
    )
    text = unescape(text)
    if strip_markup or ("<" in text and ">" in text):
        text = HTML_TAG_PATTERN.sub(" ", text)
    return normalize_space(text)


def clean_event_notes(value: str | None, *, max_length: int = 240) -> tuple[str, list[str]]:
    raw = value or ""
    flags: list[str] = []
    had_markup = bool(HTML_TAG_PATTERN.search(raw)) or bool(HTML_ESCAPE_PATTERN.search(raw))
    cleaned = raw
    for pattern in SCRIPT_NOISE_PATTERNS:
        if pattern.search(cleaned):
            cleaned = pattern.sub("", cleaned)
            flags.append("script_noise_removed")
    cleaned = decode_text(cleaned, strip_markup=True)
    if had_markup and cleaned:
        flags.append("sanitized_markup")
    if len(cleaned) > max_length:
        cleaned = cleaned[: max_length - 3].rstrip() + "..."
        flags.append("notes_truncated")
    return cleaned, normalize_quality_flags(flags)


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
        if any(value == "Event" or value.endswith("Event") for value in types if isinstance(value, str)):
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


def combine_event_range(target_date: date, start_time: time, end_time: time | None) -> tuple[datetime, datetime | None]:
    start_dt = combine_local(target_date, start_time)
    if end_time is None:
        return start_dt, None
    end_dt = combine_local(target_date, end_time)
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)
    return start_dt, end_dt


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
    match = PARTIAL_TIME_RANGE_PATTERN.search(value)
    if match and (match.group(3) or match.group(6)):
        start_meridiem = match.group(3) or match.group(6)
        end_meridiem = match.group(6) or match.group(3)
        start = _build_time(match.group(1), match.group(2), start_meridiem)
        end = _build_time(match.group(4), match.group(5), end_meridiem)
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
    preferred: list[tuple[int, str]] = []
    matches: list[tuple[int, str]] = []
    for city in METRO_CITIES:
        match = re.search(rf"\b{re.escape(city)}\b", haystack, re.IGNORECASE)
        if match:
            matches.append((match.start(), city))
        preferred_match = re.search(rf"\b{re.escape(city)}\b\s*,\s*(?:AZ|Arizona)\b", haystack, re.IGNORECASE)
        if preferred_match:
            preferred.append((preferred_match.start(), city))
    if preferred:
        preferred.sort(key=lambda item: item[0])
        return preferred[0][1]
    if not matches:
        return ""
    matches.sort(key=lambda item: item[0])
    return matches[0][1]


def split_venue_and_city(value: str | None) -> tuple[str, str]:
    cleaned = decode_text(value)
    if not cleaned:
        return "", ""

    city = infer_city(cleaned)
    if not city:
        return cleaned, ""

    venue = re.sub(
        rf",?\s*{re.escape(city)}\b(?:\s*,?\s*(?:AZ|Arizona))?.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip(" ,-")
    venue = re.sub(r"(?i)^location:\s*", "", venue).strip(" ,-")
    if venue.lower() in {city.lower(), "az", "arizona"}:
        venue = ""
    if venue.lower() in {"central", "downtown", "east", "west", "north", "south"}:
        venue = ""
    return venue, city


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
    activity_kind: str | None = None,
    occurrence: int,
    weekday: int,
    start_time: time,
    end_time: time | None,
    today: date,
    months_ahead: int = 4,
    quality_flags: list[str] | None = None,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for step in range(months_ahead):
        year = today.year + ((today.month - 1 + step) // 12)
        month = ((today.month - 1 + step) % 12) + 1
        start_date = nth_weekday_of_month(year, month, weekday, occurrence)
        if start_date < today:
            continue
        start_dt, end_dt = combine_event_range(start_date, start_time, end_time)
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
                activity_kind=activity_kind,
                quality_flags=quality_flags,
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
    activity_kind: str | None = None,
    weekday: int,
    start_time: time,
    end_time: time | None,
    today: date,
    weeks_ahead: int = 8,
    quality_flags: list[str] | None = None,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    offset = (weekday - today.weekday()) % 7
    first_date = today + timedelta(days=offset)
    for step in range(weeks_ahead):
        start_date = first_date + timedelta(days=step * 7)
        start_dt, end_dt = combine_event_range(start_date, start_time, end_time)
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
                activity_kind=activity_kind,
                quality_flags=quality_flags,
            )
        )
    return events


def event_from_jsonld(
    payload: dict[str, object],
    *,
    source_name: str,
    source_url: str,
    default_style: str,
    quality_flags: list[str] | None = None,
) -> dict[str, object] | None:
    title = decode_text(str(payload.get("name", "")), strip_markup=True)
    start_dt = parse_iso_datetime(payload.get("startDate"))  # type: ignore[arg-type]
    if not title or not start_dt:
        return None
    end_dt = parse_iso_datetime(payload.get("endDate"))  # type: ignore[arg-type]
    location = payload.get("location")
    venue = ""
    city = ""
    if isinstance(location, dict):
        venue = decode_text(str(location.get("name", "")), strip_markup=True)
        address = location.get("address")
        if isinstance(address, dict):
            city = decode_text(str(address.get("addressLocality", "")), strip_markup=True)
            venue = venue or decode_text(str(address.get("streetAddress", "")), strip_markup=True)
        elif isinstance(address, str):
            city = infer_city(address)
    description, note_flags = clean_event_notes(str(payload.get("description", "")))
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
        quality_flags=[*(quality_flags or []), *note_flags],
    )


def normalize_for_match(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", normalize_space(value).lower()).strip()


TITLE_ALIAS_GROUPS = (
    frozenset(
        {
            normalize_for_match("East Coast Swing + Social Dancing – Fatcat"),
            normalize_for_match("Triple Step Tuesdays"),
        }
    ),
)


def _titles_match_for_dedup(left: str, right: str) -> bool:
    if token_similarity(left, right) >= 0.75 or left in right or right in left:
        return True
    return any(left in aliases and right in aliases for aliases in TITLE_ALIAS_GROUPS)


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


def parse_ics_events(ics_text: str) -> list[dict[str, str]]:
    unfolded_lines: list[str] = []
    for raw_line in ics_text.splitlines():
        if raw_line.startswith((" ", "\t")) and unfolded_lines:
            unfolded_lines[-1] += raw_line[1:]
        else:
            unfolded_lines.append(raw_line.rstrip())

    events: list[dict[str, str]] = []
    current: dict[str, str] | None = None

    for line in unfolded_lines:
        if line == "BEGIN:VEVENT":
            current = {}
            continue
        if line == "END:VEVENT":
            if current:
                events.append(current)
            current = None
            continue
        if current is None or ":" not in line:
            continue
        key, value = line.split(":", 1)
        if key in current:
            current[key] = f"{current[key]},{value}"
        else:
            current[key] = value

    return events


def parse_ics_datetime(value: str, key: str) -> datetime | date | None:
    if not value:
        return None
    if "VALUE=DATE" in key:
        return datetime.strptime(value, "%Y%m%d").date()
    if value.endswith("Z"):
        return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=ZoneInfo("UTC")).astimezone(PHOENIX_TZ)
    if "TZID=America/Phoenix" in key:
        return datetime.strptime(value, "%Y%m%dT%H%M%S").replace(tzinfo=PHOENIX_TZ)
    if len(value) == 8:
        return datetime.strptime(value, "%Y%m%d").date()
    return datetime.strptime(value, "%Y%m%dT%H%M%S").replace(tzinfo=PHOENIX_TZ)


def parse_ics_rrule(value: str) -> dict[str, str]:
    rule: dict[str, str] = {}
    for segment in value.split(";"):
        if "=" not in segment:
            continue
        key, item = segment.split("=", 1)
        rule[key.upper()] = item
    return rule


def expand_ics_occurrences(
    entry: dict[str, str],
    *,
    today: date,
    horizon_days: int = 180,
) -> list[tuple[datetime | date, datetime | date | None]]:
    start_key = next((key for key in entry if key.startswith("DTSTART")), "")
    end_key = next((key for key in entry if key.startswith("DTEND")), "")
    start_value = parse_ics_datetime(entry.get(start_key, ""), start_key)
    end_value = parse_ics_datetime(entry.get(end_key, ""), end_key) if end_key else None
    if start_value is None:
        return []

    if "RRULE" not in entry:
        if _ics_occurrence_is_future(start_value, today):
            return [(start_value, end_value)]
        return []

    rule = parse_ics_rrule(entry["RRULE"])
    freq = rule.get("FREQ", "").upper()
    if freq not in {"WEEKLY", "MONTHLY"}:
        if _ics_occurrence_is_future(start_value, today):
            return [(start_value, end_value)]
        return []

    interval = int(rule.get("INTERVAL", "1") or "1")
    count_limit = int(rule.get("COUNT", "0") or "0")
    until_value = _parse_ics_until(rule.get("UNTIL", ""))
    excluded = _parse_ics_exdates(entry)
    end_date_limit = today + timedelta(days=horizon_days)
    if isinstance(until_value, datetime):
        end_date_limit = min(end_date_limit, until_value.date())
    elif isinstance(until_value, date):
        end_date_limit = min(end_date_limit, until_value)

    if isinstance(start_value, datetime):
        duration = end_value - start_value if isinstance(end_value, datetime) else None
    elif isinstance(start_value, date) and isinstance(end_value, date):
        duration = end_value - start_value
    else:
        duration = None

    generation_start = start_value.date() if count_limit else today

    if freq == "WEEKLY":
        dates = _expand_weekly_ics_dates(start_value, today=generation_start, end_date_limit=end_date_limit, rule=rule, interval=interval)
    else:
        dates = _expand_monthly_ics_dates(start_value, today=generation_start, end_date_limit=end_date_limit, rule=rule, interval=interval)

    occurrences: list[tuple[datetime | date, datetime | date | None]] = []
    for occurrence_start in dates:
        if _ics_occurrence_key(occurrence_start) in excluded:
            continue
        occurrence_end = occurrence_start + duration if duration is not None else None
        occurrences.append((occurrence_start, occurrence_end))
    if count_limit:
        occurrences = occurrences[:count_limit]
    return [
        (occurrence_start, occurrence_end)
        for occurrence_start, occurrence_end in occurrences
        if _ics_occurrence_is_future(occurrence_start, today)
    ]


def _ics_occurrence_is_future(value: datetime | date, today: date) -> bool:
    if isinstance(value, datetime):
        return value.date() >= today
    return value >= today


def _parse_ics_until(value: str) -> datetime | date | None:
    if not value:
        return None
    if value.endswith("Z"):
        return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=ZoneInfo("UTC")).astimezone(PHOENIX_TZ)
    if len(value) == 8:
        return datetime.strptime(value, "%Y%m%d").date()
    return datetime.strptime(value, "%Y%m%dT%H%M%S").replace(tzinfo=PHOENIX_TZ)


def _parse_ics_exdates(entry: dict[str, str]) -> set[str]:
    excluded: set[str] = set()
    for key, raw_value in entry.items():
        if not key.startswith("EXDATE"):
            continue
        for item in raw_value.split(","):
            parsed = parse_ics_datetime(item, key)
            if parsed is not None:
                excluded.add(_ics_occurrence_key(parsed))
    return excluded


def _ics_occurrence_key(value: datetime | date) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return value.isoformat()


def _expand_weekly_ics_dates(
    start_value: datetime | date,
    *,
    today: date,
    end_date_limit: date,
    rule: dict[str, str],
    interval: int,
) -> list[datetime | date]:
    start_date = start_value.date() if isinstance(start_value, datetime) else start_value
    if start_date > end_date_limit:
        return []
    weekday_tokens = [token for token in rule.get("BYDAY", "").split(",") if token]
    weekdays = [ICS_WEEKDAY_MAP[token[-2:]] for token in weekday_tokens if token[-2:] in ICS_WEEKDAY_MAP]
    if not weekdays:
        weekdays = [start_date.weekday()]

    occurrences: list[datetime | date] = []
    cursor = max(today, start_date)
    while cursor <= end_date_limit:
        if cursor.weekday() in weekdays:
            weeks_since_start = (cursor - start_date).days // 7
            if weeks_since_start >= 0 and weeks_since_start % interval == 0:
                occurrences.append(_ics_value_at_date(start_value, cursor))
        cursor += timedelta(days=1)
    return occurrences


def _expand_monthly_ics_dates(
    start_value: datetime | date,
    *,
    today: date,
    end_date_limit: date,
    rule: dict[str, str],
    interval: int,
) -> list[datetime | date]:
    start_date = start_value.date() if isinstance(start_value, datetime) else start_value
    if start_date > end_date_limit:
        return []

    byday_tokens = [token for token in rule.get("BYDAY", "").split(",") if token]
    occurrences: list[datetime | date] = []
    month_step = 0
    while True:
        year = start_date.year + ((start_date.month - 1 + month_step) // 12)
        month = ((start_date.month - 1 + month_step) % 12) + 1
        month_start = date(year, month, 1)
        if month_start > end_date_limit:
            break
        if month_step % interval == 0:
            candidate_dates = _monthly_candidate_dates(start_date, year=year, month=month, byday_tokens=byday_tokens)
            for candidate_date in candidate_dates:
                if candidate_date < today or candidate_date < start_date or candidate_date > end_date_limit:
                    continue
                occurrences.append(_ics_value_at_date(start_value, candidate_date))
        month_step += 1
    return occurrences


def _monthly_candidate_dates(start_date: date, *, year: int, month: int, byday_tokens: list[str]) -> list[date]:
    if not byday_tokens:
        try:
            return [date(year, month, start_date.day)]
        except ValueError:
            return []

    candidates: list[date] = []
    for token in byday_tokens:
        match = re.fullmatch(r"([+-]?\d{1,2})?([A-Z]{2})", token)
        if not match:
            continue
        occurrence_text, weekday_code = match.groups()
        weekday = ICS_WEEKDAY_MAP.get(weekday_code)
        if weekday is None:
            continue
        if not occurrence_text:
            continue
        occurrence = int(occurrence_text)
        if occurrence > 0:
            candidate = nth_weekday_of_month(year, month, weekday, occurrence)
            if candidate.month == month:
                candidates.append(candidate)
            continue
        candidate = _nth_weekday_from_end(year, month, weekday, abs(occurrence))
        if candidate.month == month:
            candidates.append(candidate)
    return candidates


def _nth_weekday_from_end(year: int, month: int, weekday: int, occurrence: int) -> date:
    if month == 12:
        candidate = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        candidate = date(year, month + 1, 1) - timedelta(days=1)
    while candidate.weekday() != weekday:
        candidate -= timedelta(days=1)
    return candidate - timedelta(days=(occurrence - 1) * 7)


def _ics_value_at_date(template: datetime | date, event_date: date) -> datetime | date:
    if isinstance(template, datetime):
        return datetime.combine(event_date, template.timetz())
    return event_date


def deduplicate_events(events: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    deduped: list[dict[str, object]] = []
    for candidate in sorted(events, key=lambda event: (str(event["start_at"]), str(event["title"]))):
        match = next((existing for existing in deduped if _likely_same_event(existing, candidate)), None)
        if match is None:
            deduped.append(candidate)
            continue
        match["venue"] = _prefer_richer_text(str(match.get("venue", "")), str(candidate.get("venue", "")))
        match["city"] = _prefer_richer_text(str(match.get("city", "")), str(candidate.get("city", "")))
        match["end_at"] = match.get("end_at") or candidate.get("end_at")
        match["source_url"] = _prefer_richer_text(str(match.get("source_url", "")), str(candidate.get("source_url", "")))
        match["notes"] = merge_notes(str(match["notes"]), str(candidate["notes"]))
        merged_flags = normalize_quality_flags(
            [
                *list(match.get("quality_flags") or []),
                *list(candidate.get("quality_flags") or []),
            ]
        )
        match["quality_flags"] = merged_flags
        existing_quality_note = normalize_space(match.get("quality_note") if isinstance(match.get("quality_note"), str) else "")
        candidate_quality_note = normalize_space(candidate.get("quality_note") if isinstance(candidate.get("quality_note"), str) else "")
        match["quality_note"] = (
            existing_quality_note
            or candidate_quality_note
            or quality_note_for_flags(merged_flags)
        )
        if match["source_name"] != candidate["source_name"]:
            match["notes"] = merge_notes(
                str(match["notes"]),
                f"Also listed on {candidate['source_name']}.",
            )
    return deduped


def _prefer_richer_text(left: str, right: str) -> str:
    left_clean = normalize_space(left)
    right_clean = normalize_space(right)
    if not left_clean:
        return right_clean
    if not right_clean:
        return left_clean
    if token_similarity(left_clean, right_clean) >= 0.95:
        return right_clean if len(right_clean) > len(left_clean) else left_clean
    return right_clean if len(right_clean) > len(left_clean) else left_clean


def _likely_same_event(left: dict[str, object], right: dict[str, object]) -> bool:
    if left["start_at"] != right["start_at"]:
        return False
    left_title = normalize_for_match(str(left["title"]))
    right_title = normalize_for_match(str(right["title"]))
    title_close = _titles_match_for_dedup(left_title, right_title)
    place_close = token_similarity(str(left["venue"]), str(right["venue"])) >= 0.75 or (
        normalize_for_match(str(left["city"])) and normalize_for_match(str(left["city"])) == normalize_for_match(str(right["city"]))
    )
    return title_close and place_close
