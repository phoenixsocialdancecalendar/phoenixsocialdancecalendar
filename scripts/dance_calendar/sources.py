from __future__ import annotations

import json
import re
import subprocess
import tempfile
from calendar import Calendar, month_name
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from html import unescape
from pathlib import Path
from typing import Callable
from urllib.parse import quote, unquote, urlencode
from urllib.request import Request, urlopen

from dance_calendar.models import make_event, normalize_space
from dance_calendar.parsing import (
    DATE_PATTERN,
    MONTH_PATTERN,
    WEEKDAY_MAP,
    clean_event_notes,
    combine_event_range,
    decode_text,
    deduplicate_events,
    expand_ics_occurrences,
    event_from_jsonld,
    expand_monthly_occurrences,
    expand_weekly_occurrences,
    extract_jsonld_events,
    extract_links,
    extract_text_lines,
    infer_city,
    is_future_event,
    parse_ics_datetime,
    parse_ics_events,
    parse_date_label,
    parse_iso_datetime,
    parse_time_range,
    serialize_dt,
    split_venue_and_city,
    strip_html,
)

SWING_DANCING_PHOENIX_API = "https://www.swingdancingphoenix.com/wp-json/tribe/events/v1/events"
SALSA_VIDA_CALENDAR_URL = "https://www.salsavida.com/guides/arizona/phoenix/calendar/"
PHXTMD_URL = "https://phxtmd.org/contra-dance"
PHXTMD_ENGLISH_URL = "https://phxtmd.org/english-dance"
PHXTMD_SPECIAL_URL = "https://phxtmd.org/special-events"
GREATER_PHOENIX_SWING_URL = "https://greaterphoenixswingdanceclub.com/calendar"
PHOENIX_SALSA_DANCE_URL = "https://phoenixsalsadance.com/calendar/"
DAVE_AND_BUSTERS_TEMPE_URL = "https://www.daveandbusters.com/us/en/about/locations/tempe"
BACHATA_ADDICTION_URL = "https://phoenixbachata.com/"
SCOOTIN_BOOTS_URL = "https://www.scootinboots.com/group-classes"
DANCEWISE_CLASSES_URL = "https://www.dancewise.com/group-ballroom-classes"
FATCAT_MEETUP_URL = "https://www.meetup.com/swing-social-dance-in-phoenix-at-fatcat-ballroom/"
PHOENIX_4TH_URL = "https://phoenix4thofjuly.com/"
RSCDS_CLASSES_URL = "https://www.rscdsphoenix.com/p/classes-2.html"
SHALL_WE_DANCE_URL = "https://shallwedancephoenix.com/calendar"
SHALL_WE_DANCE_ICS_URL = "https://calendar.google.com/calendar/ical/info%40shallwedancephoenix.com/public/basic.ics"
ENGLISH_COUNTRY_URL = "https://peghesley.com/english-country-dancing"
PHOENIX_ARGENTINE_TANGO_URL = "https://phoenixargentinetango.com/"
ZOOK_PHOENIX_URL = "https://zoukphoenix.com/calendar"
FATCAT_BALLROOM_URL = "https://www.fatcatballroom.com/dance-classes-phoenix"
FATCAT_SALSA_URL = "https://www.fatcatballroom.com/phoenix-dance-studio-adult-classes/salsa-fever-sundays"
FATCAT_ARGENTINE_TANGO_URL = "https://www.fatcatballroom.com/phoenix-dance-studio-adult-classes/argentine-tango-mondays"
FATCAT_MONDAY_SMOOTH_URL = "https://www.fatcatballroom.com/monday-smooth-dance-classes-phoenix"
FATCAT_TRIPLE_STEP_URL = "https://www.fatcatballroom.com/phoenix-dance-studio-adult-classes/triple-step-tuesdays"
FATCAT_MIDWEEK_BALLROOM_URL = "https://www.fatcatballroom.com/phoenix-dance-studio-adult-classes/mid-week-ballroom-wednesdays"
FATCAT_LINE_DANCING_URL = "https://www.fatcatballroom.com/phoenix-dance-studio-adult-classes/line-dancing-fridays"
FATCAT_WEST_COAST_SWING_URL = "https://www.fatcatballroom.com/phoenix-dance-studio-adult-classes/west-coast-swing-fridays"
WHITE_RABBIT_WCS_URL = "https://whiterabbitwcs.com/events/"
CDC_CALENDAR_URL = "https://cdc.dance/calendar/"
DESERT_CITY_SWING_URL = "https://desertcityswing.com/weekly-dance"
AZSALSA_TUMBAO_URL = "https://azsalsa.net/tumbao/"
SWINGDEPENDANCE_URL = "https://swingdependance.com/"
LATIN_SOL_URL = "https://www.latinsolfestival.com/the-event"
SUMMER_SWING_FEST_URL = "https://www.summerswingfest.com/"
PHXTMD_NUMERIC_DATE_PATTERN = re.compile(r"\d{1,2}/\d{1,2}/(?:\d{2}|\d{4})")
WHITE_RABBIT_CARD_PATTERN = re.compile(
    r'<article class="event-card"[^>]*data-event-id="(?P<event_id>[^"]+)"[^>]*>.*?'
    r'<span class="event-type"[^>]*>\s*(?P<event_type>[^<]+?)\s*</span>.*?'
    r'<h3 class="event-title"[^>]*>\s*(?P<title>.*?)\s*</h3>.*?'
    r'<div class="detail-item"[^>]*>\s*<span class="detail-icon"[^>]*>🕰️</span>\s*<span[^>]*>(?P<time_text>.*?)</span>.*?'
    r'<button class="venue-link"[^>]*data-address="(?P<address>[^"]*)"[^>]*>\s*(?P<venue>.*?)\s*</button>.*?'
    r'<span class="event-organizer"[^>]*>\s*by\s*(?P<organizer>.*?)\s*</span>.*?</article>\s*'
    r'<dialog data-maps-dialog="(?P=event_id)"[^>]*>.*?</dialog>\s*'
    r'<dialog data-event-dialog="(?P=event_id)"[^>]*>.*?<div class="dialog-description"[^>]*>\s*<p[^>]*>(?P<description>.*?)</p>',
    re.IGNORECASE | re.DOTALL,
)
CDC_CALENDAR_IMAGE_PATTERN = re.compile(
    r'https://cdc\.dance/wp-content/uploads/\d{4}/\d{2}/([A-Za-z]+)-(\d{4})-\d+\.(?:png|jpe?g|webp)',
    re.IGNORECASE,
)
CDC_TIME_PREFIX_PATTERN = re.compile(
    r"^(?P<time>(?:\d{1,2}(?::\d{2})?\s*(?:AM|PM)(?:\s*(?:-|to|–)\s*\d{1,2}(?::\d{2})?\s*(?:AM|PM)?)?)|(?:\d{1,2}(?::\d{2})?\s*(?:-|to|–)\s*\d{1,2}(?::\d{2})?\s*(?:AM|PM)))\s*(?P<rest>.*)$",
    re.IGNORECASE,
)
DESERT_CITY_TIME_PATTERN = re.compile(
    r"(?P<label>Beginner Lesson|Intermediate Lesson|Open Dance)</strong>.*?(?P<time>\d{1,2}:\d{2}\s*pm\s*[–-]\s*\d{1,2}:\d{2}\s*pm)",
    re.IGNORECASE | re.DOTALL,
)
AZSALSA_OVERALL_TIME_PATTERN = re.compile(r"Fridays\s+(?P<time>\d{1,2}:\d{2}\s*PM\s*[–-]\s*\d{1,2}(?::\d{2})?\s*AM)", re.IGNORECASE)
SWINGDEPENDANCE_YEAR_PATTERN = re.compile(r"SWINGdepenDANCE\s+(?P<year>\d{4})", re.IGNORECASE)
JULY_RANGE_PATTERN = re.compile(r"July\s+(?P<start>\d{1,2})(?:st|nd|rd|th)?\s*[–-]\s*(?:July\s+)?(?P<end>\d{1,2})(?:st|nd|rd|th)?", re.IGNORECASE)
LATIN_SOL_MAIN_PATTERN = re.compile(r"April\s+(?P<start>\d{1,2})\s*-\s*(?P<end>\d{1,2}),\s*Tempe\s+Arizona", re.IGNORECASE)
LATIN_SOL_PREPARTY_PATTERN = re.compile(r"Thursday\s+April\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})", re.IGNORECASE)
SUMMER_SWING_FEST_PATTERN = re.compile(
    r"August\s+(?P<start>\d{1,2})\s*-\s*(?P<end>\d{1,2})\s*,\s*(?P<year>\d{4})",
    re.IGNORECASE,
)
CDC_HEADER_TEXT = {"sun", "mon", "tue", "wed", "thu", "fri", "sat", "march", "april"}
CDC_OCR_SCRIPT = Path(__file__).resolve().parents[1] / "ocr_cdc_calendar.swift"
CDC_GRID_LEFT = 0.02
CDC_GRID_RIGHT = 0.98
CDC_GRID_TOP = 0.775
CDC_GRID_BOTTOM = 0.045
CDC_DEFAULT_VENUE = "Creative Dance Collective"
CDC_DEFAULT_CITY = "Mesa"


@dataclass(frozen=True)
class SourceDefinition:
    name: str
    url: str
    fetcher: Callable[[Callable[[str], str], date], list[dict[str, object]]]


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
        notes, note_flags = clean_event_notes(str(item.get("description", "")), max_length=220)
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
                quality_flags=["structured_source", *note_flags],
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
                quality_flags=["structured_source"],
            )
            if event and is_future_event(str(event["start_at"]), today):
                events.append(event)
    return deduplicate_events(events)


def fetch_phoenix_argentine_tango(fetch_text, today: date) -> list[dict[str, object]]:
    return _fetch_google_calendar_source(
        fetch_text,
        today,
        source_name="Phoenix Argentine Tango",
        page_url=PHOENIX_ARGENTINE_TANGO_URL,
        default_style="Argentine Tango",
        fallback_city="Phoenix",
    )


def fetch_zouk_phoenix(fetch_text, today: date) -> list[dict[str, object]]:
    return _fetch_google_calendar_source(
        fetch_text,
        today,
        source_name="Zouk Phoenix",
        page_url=ZOOK_PHOENIX_URL,
        default_style="Brazilian Zouk",
        fallback_city="Mesa",
    )


def fetch_white_rabbit_wcs(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(WHITE_RABBIT_WCS_URL)
    current_html = html.split('<section class="events-section past-events"', 1)[0]
    payloads_by_title = _whiterabbit_payloads_by_title(current_html)
    events: list[dict[str, object]] = []

    for match in WHITE_RABBIT_CARD_PATTERN.finditer(current_html):
        title = decode_text(match.group("title"), strip_markup=True)
        if not title:
            continue

        payload = _pop_whiterabbit_payload(payloads_by_title, title)
        if payload is None:
            continue

        start_dt = parse_iso_datetime(payload.get("startDate"))  # type: ignore[arg-type]
        if start_dt is None or start_dt.date() < today:
            continue

        address = decode_text(match.group("address"), strip_markup=True)
        venue = decode_text(match.group("venue"), strip_markup=True)
        parsed_venue, city = split_venue_and_city(address)
        if not city:
            city = infer_city(address) or infer_city(str(payload.get("description", "")))
        if not city:
            continue

        time_text = decode_text(match.group("time_text"), strip_markup=True)
        start_time, end_time = parse_time_range(time_text)
        quality_flags = ["structured_source"]
        if start_time is None:
            occurrence_start = start_dt
            occurrence_end = None
            quality_flags.append("fallback_time")
        else:
            occurrence_start, occurrence_end = combine_event_range(start_dt.date(), start_time, end_time)

        payload_description = decode_text(str(payload.get("description", "")), strip_markup=True)
        card_description = decode_text(match.group("description"), strip_markup=True)
        notes, note_flags = clean_event_notes(_merge_whiterabbit_notes(payload_description, card_description), max_length=320)
        organizer = decode_text(match.group("organizer"), strip_markup=True)
        if organizer and organizer.lower() != "white rabbit wcs" and organizer.lower() not in notes.lower():
            notes, note_flags = clean_event_notes(f"{notes} Organizer: {organizer}".strip(), max_length=320)

        events.append(
            make_event(
                title=title,
                start_at=serialize_dt(occurrence_start),
                end_at=serialize_dt(occurrence_end),
                venue=venue or parsed_venue,
                city=city,
                dance_style="West Coast Swing",
                source_name="White Rabbit WCS",
                source_url=WHITE_RABBIT_WCS_URL,
                notes=notes,
                activity_kind=_whiterabbit_activity_kind(match.group("event_type"), title, notes),
                quality_flags=[*quality_flags, *note_flags],
            )
        )

    return deduplicate_events(events)


def fetch_cdc_calendar(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(CDC_CALENDAR_URL)
    image_url = _cdc_calendar_image_url(html, today)
    if not image_url:
        return []

    month_start = _cdc_month_start(image_url)
    if month_start is None:
        return []

    image_bytes = _download_binary(image_url)
    observations = _run_cdc_calendar_ocr(image_bytes)
    events = _cdc_events_from_observations(observations, month_start, today=today)
    return deduplicate_events(events)


def fetch_desert_city_swing(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(DESERT_CITY_SWING_URL)
    schedule = _desert_city_schedule(html)
    if schedule is None:
        return []

    start_time, end_time = schedule
    notes = (
        "Friday night West Coast Swing at NRG Dance Studio with a 7:00 PM beginner lesson, "
        "7:45 PM intermediate lesson, and open dancing from 8:30 PM to 11:30 PM."
    )
    return expand_weekly_occurrences(
        title="Desert City Swing Friday Dance",
        source_name="Desert City Swing",
        source_url=DESERT_CITY_SWING_URL,
        venue="NRG Dance Studio",
        city="Tempe",
        dance_style="West Coast Swing",
        notes=notes,
        activity_kind="Social",
        weekday=WEEKDAY_MAP["friday"],
        start_time=start_time,
        end_time=end_time,
        today=today,
        quality_flags=["recurring_source"],
    )


def fetch_azsalsa_tumbao(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(AZSALSA_TUMBAO_URL)
    lines = extract_text_lines(html, base_url=AZSALSA_TUMBAO_URL)
    joined = " ".join(lines)
    if "TUMBAO Latin Fridays" not in joined or "EL PACIFICO Restaurant and Events Center" not in joined:
        return []

    time_match = AZSALSA_OVERALL_TIME_PATTERN.search(joined)
    if not time_match:
        return []
    start_time, end_time = parse_time_range(time_match.group("time"))
    if start_time is None:
        return []

    notes, note_flags = clean_event_notes(
        "Three complimentary salsa and bachata lessons from 9:15 PM to 10:30 PM, "
        "followed by social dancing with DJ Ben until 2:00 AM. $10 at the door, 21+."
    )
    return expand_weekly_occurrences(
        title="TUMBAO Latin Fridays",
        source_name="AZSalsa / TUMBAO Latin Fridays",
        source_url=AZSALSA_TUMBAO_URL,
        venue="EL PACIFICO Restaurant and Events Center",
        city="Mesa",
        dance_style="Salsa / Bachata",
        notes=notes,
        activity_kind="Social",
        weekday=WEEKDAY_MAP["friday"],
        start_time=start_time,
        end_time=end_time,
        today=today,
        quality_flags=["recurring_source", *note_flags],
    )


def fetch_swingdependance(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(SWINGDEPENDANCE_URL)
    lines = extract_text_lines(html, base_url=SWINGDEPENDANCE_URL)
    joined = " ".join(lines)
    year_match = SWINGDEPENDANCE_YEAR_PATTERN.search(joined)
    range_match = JULY_RANGE_PATTERN.search(joined)
    if not year_match or not range_match:
        return []

    year = int(year_match.group("year"))
    start_date = date(year, 7, int(range_match.group("start")))
    end_date = date(year, 7, int(range_match.group("end")))
    if end_date < today:
        return []

    notes, note_flags = clean_event_notes(
        "West Coast Swing workshop weekend with classes, contests, DJs, socials, and special events."
    )
    return [
        make_event(
            title="SWINGdepenDANCE",
            start_at=f"{start_date.isoformat()}T09:00:00-07:00",
            end_at=f"{end_date.isoformat()}T23:00:00-07:00",
            venue="",
            city="Phoenix",
            dance_style="West Coast Swing",
            source_name="SWINGdepenDANCE",
            source_url=SWINGDEPENDANCE_URL,
            notes=notes,
            activity_kind="Special Event",
            quality_flags=["text_source", "fallback_location", "fallback_time", *note_flags],
        )
    ]


def fetch_latin_sol(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(LATIN_SOL_URL)
    lines = extract_text_lines(html, base_url=LATIN_SOL_URL)
    joined = " ".join(lines)
    main_match = LATIN_SOL_MAIN_PATTERN.search(joined)
    year = _first_year(joined)
    if main_match is None or year is None:
        return []

    start_date = date(year, 4, int(main_match.group("start")))
    end_date = date(year, 4, int(main_match.group("end")))
    events: list[dict[str, object]] = []
    if end_date >= today:
        festival_notes, festival_flags = clean_event_notes(
            "Latin Sol Festival in Tempe with workshops, performances, socials, and the Salsanama freestyle salsa tournament."
        )
        events.append(
            make_event(
                title="Latin Sol Festival",
                start_at=f"{start_date.isoformat()}T09:00:00-07:00",
                end_at=f"{end_date.isoformat()}T23:00:00-07:00",
                venue="",
                city="Tempe",
                dance_style="Salsa / Bachata",
                source_name="Latin Sol Festival",
                source_url=LATIN_SOL_URL,
                notes=festival_notes,
                activity_kind="Special Event",
                quality_flags=["text_source", "fallback_location", "fallback_time", *festival_flags],
            )
        )

    if "Latin Sol Pre-Party @ The Duce" in joined:
        preparty_match = LATIN_SOL_PREPARTY_PATTERN.search(joined)
        if preparty_match:
            preparty_date = date(int(preparty_match.group("year")), 4, int(preparty_match.group("day")))
            if preparty_date >= today:
                preparty_notes, preparty_flags = clean_event_notes(
                    "Latin Sol pre-party at The Duce in downtown Phoenix. The public event page does not list a start time."
                )
                events.append(
                    make_event(
                        title="Latin Sol Pre-Party",
                        start_at=f"{preparty_date.isoformat()}T19:00:00-07:00",
                        end_at=f"{preparty_date.isoformat()}T23:00:00-07:00",
                        venue="The Duce",
                        city="Phoenix",
                        dance_style="Salsa / Bachata",
                        source_name="Latin Sol Festival",
                        source_url=LATIN_SOL_URL,
                        notes=preparty_notes,
                        activity_kind="Special Event",
                        quality_flags=["text_source", "fallback_time", *preparty_flags],
                    )
                )

    return deduplicate_events(events)


def fetch_summer_swing_fest(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(SUMMER_SWING_FEST_URL)
    lines = extract_text_lines(html, base_url=SUMMER_SWING_FEST_URL)
    joined = _collapse_spaced_digits(" ".join(lines))
    match = SUMMER_SWING_FEST_PATTERN.search(joined)
    if match is None or "Mesa, Arizona" not in joined:
        return []

    start_date = date(int(match.group("year")), 8, int(match.group("start")))
    end_date = date(int(match.group("year")), 8, int(match.group("end")))
    if end_date < today:
        return []

    notes, note_flags = clean_event_notes(
        "Annual Arizona swing dance festival with instructors, musicians, social dancing, and competitions."
    )
    return [
        make_event(
            title="Summer Swing Fest",
            start_at=f"{start_date.isoformat()}T09:00:00-07:00",
            end_at=f"{end_date.isoformat()}T23:00:00-07:00",
            venue="",
            city="Mesa",
            dance_style="Swing",
            source_name="Summer Swing Fest",
            source_url=SUMMER_SWING_FEST_URL,
            notes=notes,
            activity_kind="Special Event",
            quality_flags=["text_source", "fallback_location", "fallback_time", *note_flags],
        )
    ]


def fetch_phxtmd(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(PHXTMD_URL)
    lines = extract_text_lines(html, base_url=PHXTMD_URL)
    venue_aliases = _phxtmd_venue_aliases(lines)
    events: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        if not (DATE_PATTERN.fullmatch(line) or PHXTMD_NUMERIC_DATE_PATTERN.fullmatch(line)):
            continue
        event_date = _parse_phxtmd_date(line)
        if event_date is None or event_date < today:
            continue
        chunk = lines[index + 1 : index + 13]
        title = next((value for value in chunk if _is_phxtmd_title_line(value)), "") or _nearest_title(lines, index)
        chunk_text = " ".join(chunk)
        time_line = next((value for value in chunk if re.search(r"\d", value) and ("am" in value.lower() or "pm" in value.lower())), "")
        time_source = chunk_text if parse_time_range(chunk_text)[0] is not None else time_line
        start_time, end_time = parse_time_range(time_source)
        venue_line = next((value for value in chunk if _looks_like_phxtmd_venue_line(value)), "")
        resolved_location = _resolve_phxtmd_location(venue_line, venue_aliases)
        notes, note_flags = clean_event_notes(
            " ".join(
                value
                for value in chunk
                if value not in {time_line, venue_line, title, "Event Details"}
                and not DATE_PATTERN.fullmatch(value)
                and not PHXTMD_NUMERIC_DATE_PATTERN.fullmatch(value)
            )
        )
        if start_time is None:
            start_time = time(19, 0)
        venue, city = split_venue_and_city(resolved_location or venue_line)
        quality_flags = ["text_source", *note_flags]
        if not (resolved_location or venue_line):
            quality_flags.append("fallback_location")
        if time_source == "":
            quality_flags.append("fallback_time")
        start_dt = serialize_dt(parse_iso_datetime(f"{event_date.isoformat()}T{start_time.isoformat()}"))
        end_dt = serialize_dt(parse_iso_datetime(f"{event_date.isoformat()}T{(end_time or time(22, 0)).isoformat()}"))
        events.append(
            make_event(
                title=title or "Contra Dance",
                start_at=start_dt,
                end_at=end_dt,
                venue=venue or resolved_location or venue_line,
                city=city or infer_city(resolved_location, venue_line, notes),
                dance_style="Contra",
                source_name="Phoenix Traditional Music and Dance Society",
                source_url=PHXTMD_URL,
                notes=notes,
                quality_flags=quality_flags,
            )
        )
    return deduplicate_events(events)


def fetch_phxtmd_special_events(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(PHXTMD_SPECIAL_URL)
    lines = extract_text_lines(html, base_url=PHXTMD_SPECIAL_URL)
    if any(line.lower() == "no upcoming events." for line in lines):
        return []

    venue_aliases = _phxtmd_venue_aliases(lines)
    events: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        if not (DATE_PATTERN.fullmatch(line) or PHXTMD_NUMERIC_DATE_PATTERN.fullmatch(line)):
            continue
        event_date = _parse_phxtmd_date(line)
        if event_date is None or event_date < today:
            continue
        chunk = lines[index + 1 : index + 14]
        title = next((value for value in chunk if _is_phxtmd_title_line(value)), "")
        if not title:
            continue
        chunk_text = " ".join(chunk)
        time_line = next((value for value in chunk if re.search(r"\d", value) and ("am" in value.lower() or "pm" in value.lower())), "")
        time_source = chunk_text if parse_time_range(chunk_text)[0] is not None else time_line
        start_time, end_time = parse_time_range(time_source)
        venue_line = next((value for value in chunk if _looks_like_phxtmd_venue_line(value)), "")
        resolved_location = _resolve_phxtmd_location(venue_line, venue_aliases)
        notes, note_flags = clean_event_notes(
            " ".join(
                value
                for value in chunk
                if value not in {title, time_line, venue_line, "Event Details", "-"}
                and parse_time_range(value)[0] is None
                and not DATE_PATTERN.fullmatch(value)
                and not PHXTMD_NUMERIC_DATE_PATTERN.fullmatch(value)
            )
        )
        if start_time is None:
            start_time = time(19, 0)
        venue, city = split_venue_and_city(resolved_location or venue_line)
        start_dt = serialize_dt(parse_iso_datetime(f"{event_date.isoformat()}T{start_time.isoformat()}"))
        end_dt = serialize_dt(parse_iso_datetime(f"{event_date.isoformat()}T{(end_time or time(22, 0)).isoformat()}"))
        events.append(
            make_event(
                title=title,
                start_at=start_dt,
                end_at=end_dt,
                venue=venue or resolved_location or venue_line,
                city=city or infer_city(resolved_location, venue_line, notes),
                dance_style=_infer_dance_style(title, notes),
                source_name="Phoenix Traditional Music and Dance Society",
                source_url=PHXTMD_SPECIAL_URL,
                notes=notes,
                activity_kind="Special Event",
                quality_flags=["text_source", *note_flags],
            )
        )
    return deduplicate_events(events)


def fetch_greater_phoenix_swing(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(GREATER_PHOENIX_SWING_URL)
    lines = extract_text_lines(html, base_url=GREATER_PHOENIX_SWING_URL)
    headings = ["Friday Night Dance", "First Sunday Swing Dance"]
    friday_chunk = _block_after_heading(lines, "Friday Night Dance", headings)
    sunday_chunk = _block_after_heading(lines, "First Sunday Swing Dance", headings)
    friday_venue, friday_city = split_venue_and_city(_best_location_line(friday_chunk))
    sunday_venue, sunday_city = split_venue_and_city(_best_location_line(sunday_chunk))
    friday_notes = _build_notes_from_chunk(friday_chunk)
    sunday_notes = _build_notes_from_chunk(sunday_chunk)

    events = []
    events.extend(
        expand_monthly_occurrences(
            title="Friday Night Dance",
            source_name="Greater Phoenix Swing Dance Club",
            source_url=GREATER_PHOENIX_SWING_URL,
            venue=friday_venue,
            city=friday_city or "Mesa",
            dance_style="Swing",
            notes=friday_notes or "First Friday swing dance.",
            occurrence=1,
            weekday=4,
            start_time=time(19, 15),
            end_time=time(22, 30),
            today=today,
            quality_flags=["recurring_source", *(["fallback_location"] if not friday_venue else [])],
        )
    )
    events.extend(
        expand_monthly_occurrences(
            title="First Sunday Swing Dance",
            source_name="Greater Phoenix Swing Dance Club",
            source_url=GREATER_PHOENIX_SWING_URL,
            venue=sunday_venue or friday_venue,
            city=sunday_city or friday_city or "Mesa",
            dance_style="Swing",
            notes=sunday_notes or "First Sunday swing dance.",
            occurrence=1,
            weekday=6,
            start_time=time(17, 30),
            end_time=time(19, 45),
            today=today,
            quality_flags=["recurring_source", *(["fallback_location"] if not (sunday_venue or friday_venue) else [])],
        )
    )
    return events


def fetch_phoenix_salsa_dance(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(PHOENIX_SALSA_DANCE_URL)
    lines = extract_text_lines(html, base_url=PHOENIX_SALSA_DANCE_URL)
    default_venue, default_city = _phoenix_salsa_location_defaults(lines)
    widget_events = _fetch_phoenix_salsa_widget_events(
        fetch_text,
        html,
        today=today,
        default_venue=default_venue or "Phoenix Salsa Dance",
        default_city=default_city or "Phoenix",
    )
    if widget_events:
        return deduplicate_events(widget_events)

    events = _parse_phoenix_salsa_calendar_schedule(lines, today=today, default_venue=default_venue, default_city=default_city)
    return deduplicate_events(events)


def _parse_phoenix_salsa_calendar_schedule(
    lines: list[str],
    *,
    today: date,
    default_venue: str,
    default_city: str,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    weekday_indexes = [index for index, line in enumerate(lines) if line.lower() in WEEKDAY_MAP]

    for position, index in enumerate(weekday_indexes):
        line = lines[index]
        weekday = WEEKDAY_MAP.get(line.lower())
        if weekday is None:
            continue
        next_index = weekday_indexes[position + 1] if position + 1 < len(weekday_indexes) else len(lines)
        chunk_lines = lines[index + 1 : next_index]
        chunk = " ".join(chunk_lines)
        title = next((value for value in chunk_lines if _is_probable_title_line(value)), "")
        if not title:
            continue
        start_time, end_time = parse_time_range(chunk)
        if start_time is None:
            continue
        location_line = _best_location_line(chunk_lines)
        venue, city = split_venue_and_city(location_line)
        style = _infer_salsa_style(title, chunk)
        notes = _clean_phoenix_salsa_notes(chunk_lines, title=title, location_line=location_line)
        quality_flags = ["recurring_source"]
        if not location_line:
            quality_flags.append("fallback_location")
        events.extend(
            expand_weekly_occurrences(
                title=title,
                source_name="Phoenix Salsa Dance",
                source_url=PHOENIX_SALSA_DANCE_URL,
                venue=venue or default_venue,
                city=city or default_city or "Phoenix",
                dance_style=style,
                notes=notes,
                weekday=weekday,
                start_time=start_time,
                end_time=end_time,
                today=today,
                quality_flags=quality_flags,
            )
        )
    return events


def fetch_dave_and_busters_tempe(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(DAVE_AND_BUSTERS_TEMPE_URL)
    events: list[dict[str, object]] = []

    for payload in extract_jsonld_events(html):
        candidate = event_from_jsonld(
            payload,
            source_name="Dave & Buster's Tempe",
            source_url=DAVE_AND_BUSTERS_TEMPE_URL,
            default_style=_infer_dance_style(str(payload.get("name", "")), str(payload.get("description", ""))),
            quality_flags=["structured_source"],
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
        notes, note_flags = clean_event_notes(" ".join(chunk))
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
                quality_flags=["text_source", *note_flags],
            )
        )

    return deduplicate_events(events)


def fetch_bachata_addiction(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(BACHATA_ADDICTION_URL)
    lines = extract_text_lines(html, base_url=BACHATA_ADDICTION_URL)
    joined = " ".join(lines)
    events: list[dict[str, object]] = []

    if "Every Thursday!" in joined and "All levels Bachata class at 8, dancing 9-1am!" in joined:
        events.extend(
            expand_weekly_occurrences(
                title="Bachata Addiction Thursday Social",
                source_name="Bachata Addiction",
                source_url=BACHATA_ADDICTION_URL,
                venue="",
                city="Phoenix",
                dance_style="Bachata",
                notes="All levels bachata class at 8 PM, dancing from 9 PM to 1 AM.",
                weekday=WEEKDAY_MAP["thursday"],
                start_time=time(20, 0),
                end_time=time(1, 0),
                today=today,
                quality_flags=["recurring_source", "fallback_location"],
            )
        )

    if "Bachata Training On Sundays" in joined and "5:30-9pm at NRG Ballroom Tempe!" in joined:
        events.extend(
            expand_weekly_occurrences(
                title="Bachata Training Sundays",
                source_name="Bachata Addiction",
                source_url=BACHATA_ADDICTION_URL,
                venue="NRG Ballroom",
                city="Tempe",
                dance_style="Bachata",
                notes="Sunday bachata training at NRG Ballroom.",
                weekday=WEEKDAY_MAP["sunday"],
                start_time=time(17, 30),
                end_time=time(21, 0),
                today=today,
                quality_flags=["recurring_source"],
            )
        )

    for special in _parse_bachata_addiction_specials(lines, today):
        events.append(special)

    return deduplicate_events(events)


def fetch_dancewise(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(DANCEWISE_CLASSES_URL)
    lines = extract_text_lines(html, base_url=DANCEWISE_CLASSES_URL)
    joined = " ".join(lines)
    events: list[dict[str, object]] = []

    if "Saturday Night Group Ballroom Class and Social Dance" in joined:
        events.extend(
            expand_weekly_occurrences(
                title="Saturday Night Group Ballroom Class and Social Dance",
                source_name="DanceWise",
                source_url=DANCEWISE_CLASSES_URL,
                venue="DanceWise",
                city="Scottsdale",
                dance_style="Ballroom",
                notes="Beginner class at 7 PM, intermediate class at 7:45 PM, social dancing from 8:30 PM.",
                weekday=WEEKDAY_MAP["saturday"],
                start_time=time(19, 0),
                end_time=time(23, 0),
                today=today,
                quality_flags=["recurring_source"],
            )
        )

    if "Country with Mona Brandt" in joined and "Beginners class at 7:30 pm" in joined:
        events.extend(
            expand_weekly_occurrences(
                title="Country Classes with Mona Brandt",
                source_name="DanceWise",
                source_url=DANCEWISE_CLASSES_URL,
                venue="DanceWise",
                city="Scottsdale",
                dance_style="Country",
                notes="Beginners class at 7:30 PM and intermediate class at 8 PM.",
                weekday=WEEKDAY_MAP["thursday"],
                start_time=time(19, 30),
                end_time=time(21, 0),
                today=today,
                quality_flags=["recurring_source"],
            )
        )

    if "Hustle Dancing" in joined and "WEDNESDAYS with Mona Brandt" in joined:
        events.extend(
            expand_weekly_occurrences(
                title="Hustle Dancing",
                source_name="DanceWise",
                source_url=DANCEWISE_CLASSES_URL,
                venue="DanceWise",
                city="Scottsdale",
                dance_style="Hustle",
                notes="Weekly hustle lessons with Mona Brandt.",
                weekday=WEEKDAY_MAP["wednesday"],
                start_time=time(19, 0),
                end_time=time(20, 0),
                today=today,
                quality_flags=["recurring_source"],
            )
        )

    return deduplicate_events(events)


def fetch_scootin_boots(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(SCOOTIN_BOOTS_URL)
    joined = " ".join(extract_text_lines(html, base_url=SCOOTIN_BOOTS_URL))

    series = [
        {
            "title": "Morning Line Dancing",
            "weekday": WEEKDAY_MAP["monday"],
            "start_time": time(9, 0),
            "end_time": time(11, 30),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "Beginner, improver, and intermediate line dancing classes.",
            "required": ["MONDAYS", "Morning Line Dancing", "Beginner: 9:00 - 9:45 AM", "Intermediate: 10:30 - 11:30 AM"],
        },
        {
            "title": "Traditional Country Dancing With Mona Brandt",
            "weekday": WEEKDAY_MAP["monday"],
            "start_time": time(18, 0),
            "end_time": time(23, 0),
            "dance_style": "Country",
            "activity_kind": "Social",
            "notes": "Traditional country dancing lessons with rotating monthly styles and open dancing from 9 PM to 11 PM.",
            "required": ["Traditional Country Dancing Lessons With Mona Brandt", "Lessons With Mona Brandt: 6:00 - 9:00 PM", "Open Dancing", "9:00 -11:00 PM"],
        },
        {
            "title": "Evening Line Dancing",
            "weekday": WEEKDAY_MAP["monday"],
            "start_time": time(18, 0),
            "end_time": time(19, 30),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "High beginner and improver line dancing lessons with open line dancing until 7:30 PM.",
            "required": ["Evening Line Dancing", "High Beginner and Improver Lessons: 6:00 - 7:00 PM", "Open Line Dancing 7:00-7:30 PM"],
        },
        {
            "title": "Country Swing Mondays",
            "weekday": WEEKDAY_MAP["monday"],
            "start_time": time(19, 30),
            "end_time": time(23, 0),
            "dance_style": "Country Swing",
            "activity_kind": "Social",
            "notes": "Beginner and intermediate country swing lessons followed by open dancing.",
            "required": ["Country Swing", "Beginner and Intermediate Country Swing Lessons: 7:30 - 9:00 PM", "Open Dancing 9:00-11:00 PM"],
        },
        {
            "title": "Tuesday Line Dancing",
            "weekday": WEEKDAY_MAP["tuesday"],
            "start_time": time(17, 30),
            "end_time": time(23, 0),
            "dance_style": "Country",
            "activity_kind": "Social",
            "notes": "Tuesday line dancing with beginner and intermediate lessons plus open dancing.",
            "required": ["TUESDAYS", "Line Dancing", "Beginner Lessons: 6:00 - 6:45 PM", "Open Dancing: 5:30 - 6:00 PM, 6:45-7:15 PM, and 8:00 - 11:00 PM"],
        },
        {
            "title": "Arizona Two-Step Tuesdays",
            "weekday": WEEKDAY_MAP["tuesday"],
            "start_time": time(19, 0),
            "end_time": time(20, 0),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "Beginner and intermediate Arizona Two-Step lessons.",
            "required": ["Arizona Two-Step", "Beginner and Intermediate Lessons: 7:00 - 8:00 PM"],
        },
        {
            "title": "Country Swing Tuesdays",
            "weekday": WEEKDAY_MAP["tuesday"],
            "start_time": time(20, 0),
            "end_time": time(23, 0),
            "dance_style": "Country Swing",
            "activity_kind": "Social",
            "notes": "Beginner and intermediate country swing lessons followed by open dancing.",
            "required": ["Country Swing", "Beginner and Intermediate Lessons 8:00 - 9:00 PM", "Open Dancing: 9:00 - 11:00 PM"],
        },
        {
            "title": "Clogging (Fall-Spring)",
            "weekday": WEEKDAY_MAP["wednesday"],
            "start_time": time(13, 0),
            "end_time": time(15, 0),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "Wednesday clogging classes offered during fall through spring.",
            "required": ["WEDNESDAYS", "Clogging (Fall-Spring)", "Lessons: 1:00 - 3:00 PM"],
        },
        {
            "title": "Traditional Country Dancing Wednesdays",
            "weekday": WEEKDAY_MAP["wednesday"],
            "start_time": time(19, 0),
            "end_time": time(23, 0),
            "dance_style": "Country",
            "activity_kind": "Social",
            "notes": "Traditional country dancing lessons followed by open dancing.",
            "required": ["Traditional Country Dancing", "Lessons: 7:00 - 8:00 PM", "Open Dancing: 8:00 - 10:00 PM"],
        },
        {
            "title": "Wednesday Line Dancing",
            "weekday": WEEKDAY_MAP["wednesday"],
            "start_time": time(18, 30),
            "end_time": time(20, 0),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "Beginner and intermediate line dancing lessons.",
            "required": ["Line Dancing", "Beginner and Intermediate Lessons: 6:30 - 8:00 PM"],
        },
        {
            "title": "Country Swing Wednesdays",
            "weekday": WEEKDAY_MAP["wednesday"],
            "start_time": time(20, 0),
            "end_time": time(23, 0),
            "dance_style": "Country Swing",
            "activity_kind": "Social",
            "notes": "Country swing lessons and open dancing.",
            "required": ["Country Swing", "Beginner and Intermediate Lessons: 8:00 - 9:00 PM", "Open Dancing: 9:00 - 11:00 PM"],
        },
        {
            "title": "Morning Line Dancing",
            "weekday": WEEKDAY_MAP["thursday"],
            "start_time": time(10, 0),
            "end_time": time(11, 30),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "Improver and intermediate morning line dancing.",
            "required": ["THURSDAYS", "Morning Line Dancing", "Improver: 10:00 - 10:45 AM", "Intermediate: 10:45 - 11:30 AM"],
        },
        {
            "title": "Traditional Country Dancing Thursdays",
            "weekday": WEEKDAY_MAP["thursday"],
            "start_time": time(18, 30),
            "end_time": time(22, 0),
            "dance_style": "Country",
            "activity_kind": "Social",
            "notes": "Traditional country partner lessons with open dancing from 8 PM to 10 PM.",
            "required": ["Traditional Country Dancing", "Beginner and Intermediate Partner Lessons: 6:30 - 8:00 PM", "Open Dancing: 8:00 - 10:00 PM"],
        },
        {
            "title": "West Coast Swing Thursdays",
            "weekday": WEEKDAY_MAP["thursday"],
            "start_time": time(18, 30),
            "end_time": time(22, 0),
            "dance_style": "West Coast Swing",
            "activity_kind": "Social",
            "notes": "West Coast Swing lessons followed by open dancing and a combined dance floor.",
            "required": ["West Coast Swing", "Beginner and Beyond the Basics Lessons: 6:30 - 8:00 PM", "Open Dancing: 8:00 - 10:00 PM"],
        },
        {
            "title": "Evening Line Dancing",
            "weekday": WEEKDAY_MAP["thursday"],
            "start_time": time(19, 0),
            "end_time": time(20, 0),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "Student discount night beginner and improver line dancing lessons.",
            "required": ["Evening Line Dancing (Student Discount Night)", "Beginner and Improver: 7:00 - 8:00 PM"],
        },
        {
            "title": "Country Swing Thursdays",
            "weekday": WEEKDAY_MAP["thursday"],
            "start_time": time(20, 0),
            "end_time": time(23, 0),
            "dance_style": "Country Swing",
            "activity_kind": "Social",
            "notes": "Student discount night country swing lessons followed by open dancing.",
            "required": ["Country Swing (Student Discount Night)", "Beginner and Intermediate: 8:00 - 9:00 PM", "Open Dancing: 9:00 PM - 11:00 PM"],
        },
        {
            "title": "Friday Line Dancing",
            "weekday": WEEKDAY_MAP["friday"],
            "start_time": time(18, 30),
            "end_time": time(20, 0),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "Beginner and improver Friday line dancing with open dancing until 8 PM.",
            "required": ["FRIDAYS", "Line Dancing", "Lessons 6:30-7:30 PM: Beginner and Improver Line Dances", "Open Dancing 7:30-8 PM"],
        },
        {
            "title": "Traditional Country Dancing Fridays",
            "weekday": WEEKDAY_MAP["friday"],
            "start_time": time(18, 30),
            "end_time": time(20, 0),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "Beginner to high beginner partner lessons in traditional country dancing.",
            "required": ["Traditional Country Dancing", "Beginner - High Beginner Partner Lessons: 6:30 - 7:30 PM", "Open Dancing: 7:30 - 8:00 PM"],
        },
        {
            "title": "Country Swing Fridays",
            "weekday": WEEKDAY_MAP["friday"],
            "start_time": time(20, 0),
            "end_time": time(0, 0),
            "dance_style": "Country Swing",
            "activity_kind": "Social",
            "notes": "Country swing lessons plus open dancing until midnight.",
            "required": ["Country Swing", "Beginner and Intermediate Country Swing Lessons and 1 Line Dance: 8:00 - 9:00 PM", "Open Dancing: 9:00 PM - 12:00 AM"],
        },
        {
            "title": "Morning Line Dancing",
            "weekday": WEEKDAY_MAP["saturday"],
            "start_time": time(10, 0),
            "end_time": time(13, 0),
            "dance_style": "Country",
            "activity_kind": "Social",
            "notes": "Saturday morning line dancing lessons followed by open dancing.",
            "required": ["SATURDAYS", "Morning Line Dancing", "Lessons 10:00 - 11:30 AM: Beginner and Improver Line Dances", "Open Dancing 11:30 - 1:00 PM"],
        },
        {
            "title": "Evening Line Dancing",
            "weekday": WEEKDAY_MAP["saturday"],
            "start_time": time(18, 30),
            "end_time": time(20, 0),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "Saturday evening line dancing with beginner and improver lessons.",
            "required": ["Evening Line Dancing", "Lessons 6:30-7:30 PM: Beginner and Improver Line Dances", "Open Dancing 7:30-8 PM"],
        },
        {
            "title": "Traditional Country Dancing Saturdays",
            "weekday": WEEKDAY_MAP["saturday"],
            "start_time": time(18, 30),
            "end_time": time(20, 0),
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "notes": "Saturday beginner to high beginner traditional country partner lessons.",
            "required": ["Traditional Country Dancing", "Beginner - High Beginner Partner Lessons: 6:30 - 7:30 PM", "Open Dancing: 7:30 - 8:00 PM"],
        },
        {
            "title": "Country Swing Saturdays",
            "weekday": WEEKDAY_MAP["saturday"],
            "start_time": time(20, 0),
            "end_time": time(0, 0),
            "dance_style": "Country Swing",
            "activity_kind": "Social",
            "notes": "Saturday country swing lessons plus open dancing until midnight.",
            "required": ["Country Swing", "Beginner and Intermediate Country Swing Lessons and 1 Line Dance: 8:00 - 9:00 PM", "Open Dancing: 9:00 PM - 12:00 AM"],
        },
    ]

    events: list[dict[str, object]] = []
    for item in series:
        if not all(token in joined for token in item["required"]):
            continue
        events.extend(
            expand_weekly_occurrences(
                title=str(item["title"]),
                source_name="Scootin' Boots Dance Hall",
                source_url=SCOOTIN_BOOTS_URL,
                venue="Scootin' Boots Dance Hall",
                city="Mesa",
                dance_style=str(item["dance_style"]),
                notes=str(item["notes"]),
                activity_kind=str(item["activity_kind"]),
                weekday=int(item["weekday"]),
                start_time=item["start_time"],
                end_time=item["end_time"],
                today=today,
                quality_flags=["recurring_source"],
            )
        )
    return deduplicate_events(events)


def fetch_fatcat_ballroom(fetch_text, today: date) -> list[dict[str, object]]:
    series = [
        {
            "title": "Salsa Fever Sundays",
            "url": FATCAT_SALSA_URL,
            "dance_style": "Salsa / Bachata",
            "activity_kind": "Social",
            "weekday": WEEKDAY_MAP["sunday"],
            "start_time": time(18, 0),
            "end_time": time(20, 0),
            "required": ["Every Sunday 6PM", "6:00 PM", "8:00 PM"],
            "notes": "6 PM Latin lesson, 7 PM salsa class, and an 8 PM salsa, bachata, and cumbia social dance party.",
        },
        {
            "title": "Argentine Tango Mondays",
            "url": FATCAT_ARGENTINE_TANGO_URL,
            "dance_style": "Argentine Tango",
            "activity_kind": "Lesson",
            "weekday": WEEKDAY_MAP["monday"],
            "start_time": time(17, 30),
            "end_time": time(18, 0),
            "required": ["5:30PM every Monday", "5:30 PM"],
            "notes": "5:30 PM Argentine Tango lesson with Terry Schmoyer.",
        },
        {
            "title": "Smooth Night",
            "url": FATCAT_MONDAY_SMOOTH_URL,
            "dance_style": "Ballroom",
            "activity_kind": "Lesson",
            "weekday": WEEKDAY_MAP["monday"],
            "start_time": time(18, 0),
            "end_time": time(20, 0),
            "required": ["Every Monday night", "6:00–6:45 pm", "7:30–8:00 pm"],
            "notes": "6 PM beginner smooth ballroom lesson, 6:45 PM intermediate class, and 7:30 PM guided practice.",
        },
        {
            "title": "Triple Step Tuesdays",
            "url": FATCAT_TRIPLE_STEP_URL,
            "dance_style": "Swing",
            "activity_kind": "Social",
            "weekday": WEEKDAY_MAP["tuesday"],
            "start_time": time(19, 0),
            "end_time": time(21, 30),
            "required": ["Every Tuesday 7PM", "7:00 – 8:00 PM", "8:00 – 9:30 PM"],
            "notes": "7 PM East Coast Swing and Lindy Hop class followed by an 8 PM swing dance party.",
        },
        {
            "title": "Mid-Week Ballroom Wednesdays",
            "url": FATCAT_MIDWEEK_BALLROOM_URL,
            "dance_style": "Ballroom",
            "activity_kind": "Lesson",
            "weekday": WEEKDAY_MAP["wednesday"],
            "start_time": time(18, 30),
            "end_time": time(20, 0),
            "required": ["6:30PM every Wednesday", "6:30 PM", "8:00 PM"],
            "notes": "6:30 PM international ballroom lesson, 7:15 PM intermediate class, and 8 PM ballroom social dance party.",
        },
        {
            "title": "Line Dancing Fridays",
            "url": FATCAT_LINE_DANCING_URL,
            "dance_style": "Country",
            "activity_kind": "Lesson",
            "weekday": WEEKDAY_MAP["friday"],
            "start_time": time(18, 0),
            "end_time": time(19, 0),
            "required": ["6PM every Friday", "6:00 PM"],
            "notes": "6 PM country and pop line dancing class with Alison.",
        },
        {
            "title": "West Coast Swing Fridays",
            "url": FATCAT_WEST_COAST_SWING_URL,
            "dance_style": "West Coast Swing",
            "activity_kind": "Social",
            "weekday": WEEKDAY_MAP["friday"],
            "start_time": time(19, 0),
            "end_time": time(20, 30),
            "required": ["Every Friday 7PM", "7:00 PM", "8:30 PM"],
            "notes": "7 PM beginner West Coast Swing lesson, 7:45 PM intermediate lesson, and an 8:30 PM social dance.",
        },
    ]

    events: list[dict[str, object]] = []
    for item in series:
        html = fetch_text(item["url"])
        joined = " ".join(extract_text_lines(html, base_url=item["url"]))
        if not all(token in joined for token in item["required"]):
            continue
        events.extend(
            expand_weekly_occurrences(
                title=str(item["title"]),
                source_name="Fatcat Ballroom",
                source_url=str(item["url"]),
                venue="Fatcat Ballroom",
                city="Phoenix",
                dance_style=str(item["dance_style"]),
                notes=str(item["notes"]),
                activity_kind=str(item["activity_kind"]),
                weekday=int(item["weekday"]),
                start_time=item["start_time"],
                end_time=item["end_time"],
                today=today,
                quality_flags=["recurring_source"],
            )
        )
    return deduplicate_events(events)


def fetch_fatcat_meetup(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(FATCAT_MEETUP_URL)
    events: list[dict[str, object]] = []
    for payload in extract_jsonld_events(html):
        title = str(payload.get("name", ""))
        description = str(payload.get("description", ""))
        if not _looks_like_partner_dance_event(title, description):
            continue
        event = event_from_jsonld(
            payload,
            source_name="Fatcat Ballroom Meetup",
            source_url=FATCAT_MEETUP_URL,
            default_style=_infer_dance_style(title, description),
            quality_flags=["structured_source"],
        )
        if event and is_future_event(str(event["start_at"]), today):
            events.append(event)
    return deduplicate_events(events)


def fetch_phoenix_4th(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(PHOENIX_4TH_URL)
    date_match = re.search(r"July\s+(\d{1,2})\s*[–-]\s*(\d{1,2}),\s*(\d{4})", html, re.IGNORECASE)
    if not date_match:
        return []
    month = 7
    start_day = int(date_match.group(1))
    end_day = int(date_match.group(2))
    year = int(date_match.group(3))
    start_date = date(year, month, start_day)
    end_date = date(year, month, end_day)
    if end_date < today:
        return []
    notes = "West Coast Swing convention with workshops, competitions, social dancing, and late-night parties."
    return [
        make_event(
            title="Phoenix 4th of July Dance Convention",
            start_at=f"{start_date.isoformat()}T09:00:00-07:00",
            end_at=f"{end_date.isoformat()}T23:00:00-07:00",
            venue="JW Marriott Camelback Inn Resort & Spa",
            city="Scottsdale",
            dance_style="West Coast Swing",
            source_name="Phoenix 4th of July Dance Convention",
            source_url=PHOENIX_4TH_URL,
            notes=notes,
            activity_kind="Special Event",
            quality_flags=["text_source"],
        )
    ]


def fetch_rscds_phoenix(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(RSCDS_CLASSES_URL)
    lines = extract_text_lines(html, base_url=RSCDS_CLASSES_URL)
    joined = " ".join(lines)
    if "Classes are held 6:30-8 pm every Tuesday" not in joined:
        return []
    if today.month in {6, 7, 8}:
        return []
    return expand_weekly_occurrences(
        title="Scottish Country Dance Class",
        source_name="RSCDS Phoenix Branch",
        source_url=RSCDS_CLASSES_URL,
        venue="Granite Reef Senior Center, 1700 N Granite Reef Rd",
        city="Scottsdale",
        dance_style="Scottish Country",
        notes="Classes are held every Tuesday from September through May, except holidays, in room 13/14.",
        activity_kind="Lesson",
        weekday=WEEKDAY_MAP["tuesday"],
        start_time=time(18, 30),
        end_time=time(20, 0),
        today=today,
        quality_flags=["recurring_source"],
    )


def fetch_shall_we_dance(fetch_text, today: date) -> list[dict[str, object]]:
    ics_text = fetch_text(SHALL_WE_DANCE_ICS_URL)
    events: list[dict[str, object]] = []
    for entry in parse_ics_events(ics_text):
        if entry.get("STATUS", "").upper() == "CANCELLED":
            continue
        start_key = next((key for key in entry if key.startswith("DTSTART")), "")
        end_key = next((key for key in entry if key.startswith("DTEND")), "")
        start_value = parse_ics_datetime(entry.get(start_key, ""), start_key)
        end_value = parse_ics_datetime(entry.get(end_key, ""), end_key) if end_key else None
        if not isinstance(start_value, datetime):
            continue
        if start_value.date() < today:
            continue
        end_dt = end_value if isinstance(end_value, datetime) else None
        summary = decode_text(entry.get("SUMMARY", ""), strip_markup=True)
        description, note_flags = clean_event_notes(entry.get("DESCRIPTION", ""))
        location = decode_text(entry.get("LOCATION", ""), strip_markup=True)
        venue, city = split_venue_and_city(location)
        events.append(
            make_event(
                title=summary,
                start_at=serialize_dt(start_value),
                end_at=serialize_dt(end_dt),
                venue=venue or location,
                city=city or infer_city(location, description) or "Phoenix",
                dance_style=_infer_dance_style(summary, description),
                source_name="Shall We Dance Phoenix",
                source_url=decode_text(entry.get("URL", "")) or SHALL_WE_DANCE_URL,
                notes=description,
                quality_flags=["ics_source", "structured_source", *note_flags],
            )
        )
    return deduplicate_events(events)


def fetch_english_country(fetch_text, today: date) -> list[dict[str, object]]:
    try:
        phxtmd_html = fetch_text(PHXTMD_ENGLISH_URL)
    except Exception:
        phxtmd_html = ""
    if phxtmd_html:
        phxtmd_events = _fetch_phxtmd_english(fetch_text=lambda _url: phxtmd_html, today=today)
        if phxtmd_events:
            return deduplicate_events(phxtmd_events)

    html = fetch_text(ENGLISH_COUNTRY_URL)
    lines = extract_text_lines(html, base_url=ENGLISH_COUNTRY_URL)
    joined = " ".join(lines)
    if "2nd and 4th Saturday mornings" not in joined:
        return []
    venue = "Irish Cultural Center or Hesley House"
    notes = "Phoenix English Country Dancers meet 9:00 - 11:00 AM on the 2nd and 4th Saturday mornings."
    events = []
    events.extend(
        expand_monthly_occurrences(
            title="English Country Dancing",
            source_name="Phoenix English Country Dancers",
            source_url=ENGLISH_COUNTRY_URL,
            venue=venue,
            city="Phoenix",
            dance_style="English Country",
            notes=notes,
            occurrence=2,
            weekday=WEEKDAY_MAP["saturday"],
            start_time=time(9, 0),
            end_time=time(11, 0),
            today=today,
            quality_flags=["recurring_source"],
        )
    )
    events.extend(
        expand_monthly_occurrences(
            title="English Country Dancing",
            source_name="Phoenix English Country Dancers",
            source_url=ENGLISH_COUNTRY_URL,
            venue=venue,
            city="Phoenix",
            dance_style="English Country",
            notes=notes,
            occurrence=4,
            weekday=WEEKDAY_MAP["saturday"],
            start_time=time(9, 0),
            end_time=time(11, 0),
            today=today,
            quality_flags=["recurring_source"],
        )
    )
    return deduplicate_events(events)


def all_sources() -> list[SourceDefinition]:
    return [
        SourceDefinition("Swing Dancing Phoenix", SWING_DANCING_PHOENIX_API, fetch_swing_dancing_phoenix),
        SourceDefinition("White Rabbit WCS", WHITE_RABBIT_WCS_URL, fetch_white_rabbit_wcs),
        SourceDefinition("CDC Studios", CDC_CALENDAR_URL, fetch_cdc_calendar),
        SourceDefinition("Desert City Swing", DESERT_CITY_SWING_URL, fetch_desert_city_swing),
        SourceDefinition("AZSalsa / TUMBAO Latin Fridays", AZSALSA_TUMBAO_URL, fetch_azsalsa_tumbao),
        SourceDefinition("SWINGdepenDANCE", SWINGDEPENDANCE_URL, fetch_swingdependance),
        SourceDefinition("Latin Sol Festival", LATIN_SOL_URL, fetch_latin_sol),
        SourceDefinition("Summer Swing Fest", SUMMER_SWING_FEST_URL, fetch_summer_swing_fest),
        SourceDefinition("Salsa Vida", SALSA_VIDA_CALENDAR_URL, fetch_salsa_vida),
        SourceDefinition("Phoenix Argentine Tango", PHOENIX_ARGENTINE_TANGO_URL, fetch_phoenix_argentine_tango),
        SourceDefinition("Zouk Phoenix", ZOOK_PHOENIX_URL, fetch_zouk_phoenix),
        SourceDefinition("Phoenix Traditional Music and Dance Society", PHXTMD_URL, fetch_phxtmd),
        SourceDefinition("Phoenix Traditional Music and Dance Society Special Events", PHXTMD_SPECIAL_URL, fetch_phxtmd_special_events),
        SourceDefinition("Greater Phoenix Swing Dance Club", GREATER_PHOENIX_SWING_URL, fetch_greater_phoenix_swing),
        SourceDefinition("Phoenix Salsa Dance", PHOENIX_SALSA_DANCE_URL, fetch_phoenix_salsa_dance),
        SourceDefinition("Dave & Buster's Tempe", DAVE_AND_BUSTERS_TEMPE_URL, fetch_dave_and_busters_tempe),
        SourceDefinition("Bachata Addiction", BACHATA_ADDICTION_URL, fetch_bachata_addiction),
        SourceDefinition("DanceWise", DANCEWISE_CLASSES_URL, fetch_dancewise),
        SourceDefinition("Scootin' Boots Dance Hall", SCOOTIN_BOOTS_URL, fetch_scootin_boots),
        SourceDefinition("Fatcat Ballroom", FATCAT_BALLROOM_URL, fetch_fatcat_ballroom),
        SourceDefinition("Fatcat Ballroom Meetup", FATCAT_MEETUP_URL, fetch_fatcat_meetup),
        SourceDefinition("Phoenix 4th of July Dance Convention", PHOENIX_4TH_URL, fetch_phoenix_4th),
        SourceDefinition("RSCDS Phoenix Branch", RSCDS_CLASSES_URL, fetch_rscds_phoenix),
        SourceDefinition("Shall We Dance Phoenix", SHALL_WE_DANCE_URL, fetch_shall_we_dance),
        SourceDefinition("Phoenix English Country Dancers", ENGLISH_COUNTRY_URL, fetch_english_country),
    ]


def all_source_fetchers():
    return [source.fetcher for source in all_sources()]


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


def _fetch_phxtmd_english(fetch_text, today: date) -> list[dict[str, object]]:
    html = fetch_text(PHXTMD_ENGLISH_URL)
    lines = extract_text_lines(html, base_url=PHXTMD_ENGLISH_URL)
    venue_aliases = _phxtmd_venue_aliases(lines)
    events: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        if not PHXTMD_NUMERIC_DATE_PATTERN.fullmatch(line):
            continue
        event_date = _parse_phxtmd_date(line)
        if event_date is None:
            continue
        chunk = lines[index + 1 : index + 13]
        chunk_text = " ".join(chunk)
        if "ecd" not in chunk_text.lower():
            continue
        event_date = _correct_phxtmd_english_date(event_date, chunk_text)
        if event_date < today:
            continue
        time_line = next((value for value in chunk if re.search(r"\d", value) and ("am" in value.lower() or "pm" in value.lower())), "")
        time_source = chunk_text if parse_time_range(chunk_text)[0] is not None else time_line
        start_time, end_time = parse_time_range(time_source)
        if start_time is None:
            start_time = time(9, 0)
        venue_line = next((value for value in chunk if _looks_like_phxtmd_venue_line(value)), "")
        resolved_location = _resolve_phxtmd_location(venue_line, venue_aliases)
        notes, note_flags = clean_event_notes(
            " ".join(
                value
                for value in chunk
                if value not in {time_line, venue_line, "ECD", "Event Details", "-"}
                and parse_time_range(value)[0] is None
                and not PHXTMD_NUMERIC_DATE_PATTERN.fullmatch(value)
            )
        )
        venue, city = split_venue_and_city(resolved_location or venue_line)
        start_dt = serialize_dt(parse_iso_datetime(f"{event_date.isoformat()}T{start_time.isoformat()}"))
        end_dt = serialize_dt(parse_iso_datetime(f"{event_date.isoformat()}T{(end_time or time(11, 0)).isoformat()}"))
        events.append(
            make_event(
                title="English Country Dancing",
                start_at=start_dt,
                end_at=end_dt,
                venue=venue or resolved_location or venue_line,
                city=city or infer_city(resolved_location, venue_line, notes) or "Phoenix",
                dance_style="English Country",
                source_name="Phoenix English Country Dancers",
                source_url=PHXTMD_ENGLISH_URL,
                notes=notes or "2nd & 4th Saturdays English country dance.",
                quality_flags=["text_source", *note_flags],
            )
        )
    return deduplicate_events(events)


def _parse_phxtmd_date(value: str) -> date | None:
    parsed = parse_date_label(value)
    if parsed is not None:
        return parsed
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _correct_phxtmd_english_date(event_date: date, chunk_text: str) -> date:
    if event_date.weekday() == WEEKDAY_MAP["saturday"]:
        return event_date
    if "2nd & 4th saturdays" not in chunk_text.lower():
        return event_date
    occurrence = 2 if event_date.day <= 14 else 4
    month_start = date(event_date.year, event_date.month, 1)
    offset = (WEEKDAY_MAP["saturday"] - month_start.weekday()) % 7
    return month_start + timedelta(days=offset + ((occurrence - 1) * 7))


def _phxtmd_venue_aliases(lines: list[str]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for line in lines:
        match = re.match(r"^([A-Z]{2,5})(?: \(map\))? =\s*(.+)$", line)
        if not match:
            continue
        alias = match.group(1).strip()
        location = normalize_space(match.group(2))
        aliases[alias] = location
    if "GOCCC" in aliases and "GOCC" not in aliases:
        aliases["GOCC"] = aliases["GOCCC"]
    if "ICC" in aliases:
        aliases["ICC Great Hall"] = aliases["ICC"]
    return aliases


def _looks_like_phxtmd_venue_line(value: str) -> bool:
    lower = value.lower()
    return (
        infer_city(value) != ""
        or "az" in lower
        or "icc" == value.lower()
        or value.startswith(("ICC ", "GOCC", "GOCCC", "KDS", "PCM"))
        or "conservatory" in lower
        or "community center" in lower
    )


def _resolve_phxtmd_location(venue_line: str, aliases: dict[str, str]) -> str:
    cleaned = normalize_space(venue_line)
    if not cleaned:
        return ""
    for alias, location in aliases.items():
        if cleaned == alias or cleaned.startswith(f"{alias} ") or f"({alias})" in cleaned:
            if f"({alias})" in cleaned:
                label = normalize_space(cleaned.split(f"({alias})", 1)[0]).strip(" ,-")
                if label:
                    return f"{label}, {location}"
            return location
    return cleaned


def _is_phxtmd_title_line(value: str) -> bool:
    cleaned = normalize_space(value)
    if not cleaned:
        return False
    if cleaned in {"Event Details", "More Events"}:
        return False
    if PHXTMD_NUMERIC_DATE_PATTERN.fullmatch(cleaned) or DATE_PATTERN.fullmatch(cleaned):
        return False
    if parse_time_range(cleaned)[0] is not None:
        return False
    return True


def _block_after_heading(lines: list[str], heading: str, headings: list[str]) -> list[str]:
    heading_lookup = {value.lower() for value in headings}
    for index, line in enumerate(lines):
        if line.lower() != heading.lower():
            continue
        block: list[str] = []
        for candidate in lines[index + 1 :]:
            if candidate.lower() in heading_lookup:
                break
            block.append(candidate)
        return block
    return []


def _best_location_line(lines: list[str]) -> str:
    candidates = [line for line in lines if _looks_like_location_line(line)]
    if not candidates:
        return ""
    preferred = next((line for line in candidates if len(split_venue_and_city(line)[0].split()) >= 1), "")
    return preferred or candidates[0]


def _shared_location_defaults(lines: list[str]) -> tuple[str, str]:
    locations = [split_venue_and_city(line) for line in lines if _looks_like_location_line(line)]
    venues = sorted({venue for venue, _city in locations if venue})
    cities = sorted({city for _venue, city in locations if city})
    default_venue = venues[0] if len(venues) == 1 else ""
    default_city = cities[0] if len(cities) == 1 else ""
    return default_venue, default_city


def _phoenix_salsa_location_defaults(lines: list[str]) -> tuple[str, str]:
    if "Location:" in lines:
        index = lines.index("Location:")
        nearby = lines[index + 1 : index + 5]
        venue = normalize_space(nearby[0]) if nearby else ""
        city = infer_city(" ".join(nearby))
        if venue or city:
            return venue, city
    return _shared_location_defaults(lines)


def _fetch_phoenix_salsa_widget_events(
    fetch_text,
    html: str,
    *,
    today: date,
    default_venue: str,
    default_city: str,
) -> list[dict[str, object]]:
    widget_id_match = re.search(r"elfsight-app-([a-f0-9-]{36})", html, re.IGNORECASE)
    if not widget_id_match:
        return []

    widget_id = widget_id_match.group(1)
    boot_url = "https://core.service.elfsight.com/p/boot/?" + urlencode({"w": widget_id, "page": PHOENIX_SALSA_DANCE_URL})
    try:
        payload = json.loads(fetch_text(boot_url))
    except (json.JSONDecodeError, KeyError, TypeError):
        return []

    widget = (((payload.get("data") or {}).get("widgets") or {}).get(widget_id) or {})
    settings = ((widget.get("data") or {}).get("settings") or {})
    raw_events = settings.get("events")
    raw_locations = settings.get("locations")
    if not isinstance(raw_events, list):
        return []

    locations_by_id: dict[str, dict[str, object]] = {}
    if isinstance(raw_locations, list):
        for item in raw_locations:
            if not isinstance(item, dict):
                continue
            location_id = str(item.get("id", ""))
            if location_id:
                locations_by_id[location_id] = item

    events: list[dict[str, object]] = []
    for item in raw_events:
        if not isinstance(item, dict):
            continue
        events.extend(
            _elfsight_event_occurrences(
                item,
                locations_by_id=locations_by_id,
                today=today,
                default_venue=default_venue,
                default_city=default_city,
            )
        )
    return events


def _elfsight_event_occurrences(
    item: dict[str, object],
    *,
    locations_by_id: dict[str, dict[str, object]],
    today: date,
    default_venue: str,
    default_city: str,
    recurring_horizon_days: int = 90,
) -> list[dict[str, object]]:
    if item.get("visible") is False:
        return []

    start_dt = _elfsight_datetime(item.get("start"))
    end_dt = _elfsight_datetime(item.get("end"))
    title = decode_text(str(item.get("name", "")), strip_markup=True)
    if start_dt is None or not title:
        return []

    description, note_flags = clean_event_notes(str(item.get("description", "")), max_length=320)
    venue, city = _elfsight_location(item, locations_by_id, description, default_venue, default_city)
    source_url = _elfsight_source_url(item) or PHOENIX_SALSA_DANCE_URL
    dance_style = _infer_salsa_style(title, description)
    quality_flags = ["structured_source", "recurring_source", *note_flags]

    repeat_period = str(item.get("repeatPeriod") or "")
    if repeat_period in {"", "noRepeat"}:
        if start_dt.date() < today:
            return []
        return [
            make_event(
                title=title,
                start_at=serialize_dt(start_dt),
                end_at=serialize_dt(end_dt),
                venue=venue,
                city=city,
                dance_style=dance_style,
                source_name="Phoenix Salsa Dance",
                source_url=source_url,
                notes=description,
                quality_flags=[flag for flag in quality_flags if flag != "recurring_source"],
            )
        ]

    if repeat_period not in {"weeklyOn", "custom"}:
        return []

    recurrence_end = _elfsight_recurrence_end_date(item)
    if recurrence_end is not None and recurrence_end < today:
        return []

    interval = int(item.get("repeatInterval") or 1)
    if interval < 1:
        interval = 1
    weekday = start_dt.weekday()
    first_occurrence = start_dt.date()
    if first_occurrence < today:
        days_since_start = (today - first_occurrence).days
        intervals_to_today = max(0, days_since_start // (7 * interval))
        first_occurrence = first_occurrence + timedelta(days=intervals_to_today * 7 * interval)
        while first_occurrence < today:
            first_occurrence += timedelta(days=7 * interval)

    horizon_end = today + timedelta(days=recurring_horizon_days)
    if recurrence_end is not None:
        horizon_end = min(horizon_end, recurrence_end)
    if first_occurrence > horizon_end:
        return []

    duration = (end_dt - start_dt) if end_dt is not None else None
    occurrences: list[dict[str, object]] = []
    occurrence_date = first_occurrence
    while occurrence_date <= horizon_end:
        if occurrence_date.weekday() == weekday:
            occurrence_start = datetime.combine(occurrence_date, start_dt.timetz())
            occurrence_end = occurrence_start + duration if duration is not None else None
            occurrences.append(
                make_event(
                    title=title,
                    start_at=serialize_dt(occurrence_start),
                    end_at=serialize_dt(occurrence_end),
                    venue=venue,
                    city=city,
                    dance_style=dance_style,
                    source_name="Phoenix Salsa Dance",
                    source_url=source_url,
                    notes=description,
                    quality_flags=quality_flags,
                )
            )
        occurrence_date += timedelta(days=7 * interval)
    return occurrences


def _elfsight_datetime(value: object) -> datetime | None:
    if not isinstance(value, dict):
        return None
    if value.get("type") != "datetime":
        return None
    date_value = normalize_space(str(value.get("date", "")))
    time_value = normalize_space(str(value.get("time", "")))
    if not date_value or not time_value:
        return None
    return parse_iso_datetime(f"{date_value}T{time_value}:00")


def _elfsight_recurrence_end_date(item: dict[str, object]) -> date | None:
    if str(item.get("repeatEnds") or "").lower() != "ondate":
        return None
    end_dt = _elfsight_datetime(item.get("repeatEndsDate"))
    if end_dt is None:
        return None
    start_dt = _elfsight_datetime(item.get("start"))
    if start_dt is not None and end_dt.date() < start_dt.date():
        return None
    return end_dt.date()


def _elfsight_location(
    item: dict[str, object],
    locations_by_id: dict[str, dict[str, object]],
    description: str,
    default_venue: str,
    default_city: str,
) -> tuple[str, str]:
    location_ids = item.get("location")
    if isinstance(location_ids, list):
        for location_id in location_ids:
            location = locations_by_id.get(str(location_id))
            if not location:
                continue
            venue = normalize_space(str(location.get("name", "")))
            address = normalize_space(str(location.get("address", "")))
            parsed_venue, parsed_city = split_venue_and_city(", ".join(part for part in [venue, address] if part))
            if parsed_venue or parsed_city:
                return parsed_venue or venue, parsed_city or infer_city(address, description) or default_city

    if "phoenix salsa dance" in description.lower():
        return default_venue, default_city
    return "", infer_city(description) or default_city


def _elfsight_source_url(item: dict[str, object]) -> str:
    button_link = item.get("buttonLink")
    if isinstance(button_link, dict):
        value = normalize_space(str(button_link.get("value") or button_link.get("rawValue") or ""))
        if value:
            return value
    return ""


def _build_notes_from_chunk(lines: list[str]) -> str:
    location_line = _best_location_line(lines)
    parts = [decode_text(line, strip_markup=True) for line in lines if line and line != location_line]
    notes, _ = clean_event_notes(" ".join(part for part in parts if part))
    return notes


def _desert_city_schedule(html: str) -> tuple[time, time | None] | None:
    times_by_label: dict[str, tuple[time, time | None]] = {}
    for match in DESERT_CITY_TIME_PATTERN.finditer(html):
        parsed = parse_time_range(match.group("time"))
        if parsed[0] is None:
            continue
        times_by_label[normalize_space(match.group("label")).lower()] = parsed

    beginner = times_by_label.get("beginner lesson")
    open_dance = times_by_label.get("open dance")
    if beginner is None or open_dance is None:
        return None
    return beginner[0], open_dance[1]


def _first_year(value: str) -> int | None:
    match = re.search(r"\b(20\d{2})\b", value)
    return int(match.group(1)) if match else None


def _collapse_spaced_digits(value: str) -> str:
    collapsed = value
    while True:
        updated = re.sub(r"(?<=\d)\s+(?=\d)", "", collapsed)
        if updated == collapsed:
            return updated
        collapsed = updated


def _whiterabbit_payloads_by_title(html: str) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for payload in extract_jsonld_events(html):
        title = decode_text(str(payload.get("name", "")), strip_markup=True)
        start_dt = parse_iso_datetime(payload.get("startDate"))  # type: ignore[arg-type]
        if not title or start_dt is None:
            continue
        key = _whiterabbit_title_key(title)
        grouped.setdefault(key, []).append(payload)
    return grouped


def _pop_whiterabbit_payload(payloads_by_title: dict[str, list[dict[str, object]]], title: str) -> dict[str, object] | None:
    queue = payloads_by_title.get(_whiterabbit_title_key(title), [])
    if not queue:
        return None
    return queue.pop(0)


def _whiterabbit_title_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", normalize_space(value).lower()).strip()


def _merge_whiterabbit_notes(primary: str, secondary: str) -> str:
    parts = [normalize_space(primary), normalize_space(secondary)]
    unique: list[str] = []
    for part in parts:
        if part and part not in unique:
            unique.append(part)
    return " ".join(unique)


def _whiterabbit_activity_kind(raw_type: str, title: str, notes: str) -> str:
    normalized = normalize_space(raw_type).lower()
    if normalized == "social":
        return "Social"
    if normalized == "class":
        return "Lesson"
    if normalized in {"workshop", "competition"}:
        return "Special Event"
    return "Social" if "social" in f"{title} {notes}".lower() else "Lesson"


def _cdc_calendar_image_url(html: str, today: date) -> str:
    candidates: list[tuple[date, str]] = []
    for match in CDC_CALENDAR_IMAGE_PATTERN.finditer(html):
        month_name_value = match.group(1)
        year = int(match.group(2))
        month_index = _month_index(month_name_value)
        if month_index is None:
            continue
        url = match.group(0)
        candidates.append((date(year, month_index, 1), url))
    if not candidates:
        return ""

    candidates.sort(key=lambda item: item[0])
    current_month = date(today.year, today.month, 1)
    future = [item for item in candidates if item[0] >= current_month]
    return future[0][1] if future else candidates[-1][1]


def _cdc_month_start(image_url: str) -> date | None:
    match = CDC_CALENDAR_IMAGE_PATTERN.search(image_url)
    if not match:
        return None
    month_index = _month_index(match.group(1))
    if month_index is None:
        return None
    return date(int(match.group(2)), month_index, 1)


def _month_index(name: str) -> int | None:
    cleaned = normalize_space(name).lower()
    for index in range(1, 13):
        if month_name[index].lower() == cleaned:
            return index
    return None


def _download_binary(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "PhoenixDanceCalendarBot/1.0 (+https://example.com)", "Accept": "*/*"})
    with urlopen(request, timeout=30) as response:
        return response.read()


def _run_cdc_calendar_ocr(image_bytes: bytes) -> list[dict[str, object]]:
    with tempfile.NamedTemporaryFile(suffix=".jpg") as handle:
        handle.write(image_bytes)
        handle.flush()
        result = subprocess.run(
            ["swift", str(CDC_OCR_SCRIPT), handle.name],
            capture_output=True,
            text=True,
            check=True,
        )
    observations: list[dict[str, object]] = []
    for raw_line in result.stdout.splitlines():
        parts = raw_line.split("|", 4)
        if len(parts) != 5:
            continue
        try:
            x, y, width, height = (float(value) for value in parts[:4])
        except ValueError:
            continue
        text = normalize_space(parts[4])
        if not text:
            continue
        observations.append({"x": x, "y": y, "width": width, "height": height, "text": text})
    return observations


def _cdc_events_from_observations(
    observations: list[dict[str, object]],
    month_start: date,
    *,
    today: date,
) -> list[dict[str, object]]:
    calendar_weeks = Calendar(firstweekday=6).monthdayscalendar(month_start.year, month_start.month)
    row_count = len(calendar_weeks)
    row_height = (CDC_GRID_TOP - CDC_GRID_BOTTOM) / row_count
    col_width = (CDC_GRID_RIGHT - CDC_GRID_LEFT) / 7

    events: list[dict[str, object]] = []
    for row_index, week in enumerate(calendar_weeks):
        for col_index, day_number in enumerate(week):
            if day_number == 0:
                continue
            event_date = date(month_start.year, month_start.month, day_number)
            if event_date < today:
                continue
            cell_lines = _cdc_cell_lines(
                observations,
                left=CDC_GRID_LEFT + (col_index * col_width),
                right=CDC_GRID_LEFT + ((col_index + 1) * col_width),
                top=CDC_GRID_TOP - (row_index * row_height),
                bottom=CDC_GRID_TOP - ((row_index + 1) * row_height),
                day_number=day_number,
            )
            events.extend(_cdc_events_for_day(cell_lines, event_date))
    return events


def _cdc_cell_lines(
    observations: list[dict[str, object]],
    *,
    left: float,
    right: float,
    top: float,
    bottom: float,
    day_number: int,
) -> list[str]:
    lines: list[tuple[float, float, str]] = []
    for observation in observations:
        center_x = float(observation["x"]) + (float(observation["width"]) / 2)
        center_y = float(observation["y"]) + (float(observation["height"]) / 2)
        text = str(observation["text"])
        if not (left <= center_x <= right and bottom <= center_y <= top):
            continue
        if text == str(day_number):
            continue
        if text.lower() in CDC_HEADER_TEXT:
            continue
        if len(text) <= 2 and text.isdigit():
            continue
        lines.append((center_y, center_x, text))

    lines.sort(key=lambda item: (-item[0], item[1]))
    return _cdc_compact_lines([text for _y, _x, text in lines])


def _cdc_compact_lines(lines: list[str]) -> list[str]:
    compacted: list[str] = []
    for line in lines:
        text = normalize_space(line)
        if not text:
            continue
        if compacted and _cdc_line_has_time(compacted[-1]) and not _cdc_line_has_time(text):
            compacted[-1] = normalize_space(f"{compacted[-1]} {text}")
            continue
        if compacted and not _cdc_line_has_time(compacted[-1]) and not _cdc_line_has_time(text):
            compacted[-1] = normalize_space(f"{compacted[-1]} {text}")
            continue
        compacted.append(text)
    return compacted


def _cdc_events_for_day(lines: list[str], event_date: date) -> list[dict[str, object]]:
    if not lines:
        return []

    prefix = ""
    events: list[dict[str, object]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        parsed = _cdc_parse_time_prefix(line)
        if parsed is None:
            prefix = line
            index += 1
            continue

        start_time, end_time, remainder = parsed
        title = remainder
        if not title and index + 1 < len(lines) and not _cdc_line_has_time(lines[index + 1]):
            title = lines[index + 1]
            index += 1

        if prefix and _cdc_should_apply_prefix(prefix, title):
            title = normalize_space(f"{prefix} {title}")

        event = _cdc_make_event(event_date, title, start_time, end_time)
        if event is not None:
            events.append(event)
        index += 1

    if events:
        return events

    if any(keyword in " ".join(lines).lower() for keyword in ["workshop", "social", "dance party", "class"]):
        event = _cdc_make_text_only_event(event_date, " ".join(lines))
        return [event] if event is not None else []
    return []


def _cdc_parse_time_prefix(line: str) -> tuple[time, time | None, str] | None:
    match = CDC_TIME_PREFIX_PATTERN.match(line)
    if not match:
        return None
    time_text = normalize_space(match.group("time"))
    remainder = normalize_space(match.group("rest"))
    parsed_range = parse_time_range(time_text)
    start_time = parsed_range[0]
    end_time = parsed_range[1]
    if start_time is None:
        return None
    return start_time, end_time, remainder


def _cdc_line_has_time(line: str) -> bool:
    return _cdc_parse_time_prefix(line) is not None


def _cdc_should_apply_prefix(prefix: str, title: str) -> bool:
    if not prefix:
        return False
    lowered_title = title.lower()
    if lowered_title.startswith(prefix.lower()):
        return False
    if any(keyword in prefix.lower() for keyword in ["west coast swing", "wcs"]):
        return True
    if lowered_title in {"beginner", "intermediate", "social"}:
        return True
    return len(title.split()) <= 2


def _cdc_make_event(event_date: date, title: str, start_time: time, end_time: time | None) -> dict[str, object] | None:
    normalized_title = normalize_space(title)
    if not normalized_title or normalized_title.lower() in {"lucky"}:
        return None
    start_dt, end_dt = combine_event_range(event_date, start_time, end_time)
    notes, note_flags = clean_event_notes("Parsed from CDC monthly calendar image.", max_length=120)
    return make_event(
        title=normalized_title,
        start_at=serialize_dt(start_dt),
        end_at=serialize_dt(end_dt),
        venue=CDC_DEFAULT_VENUE,
        city=CDC_DEFAULT_CITY,
        dance_style=_cdc_infer_style(normalized_title),
        source_name="CDC Studios",
        source_url=CDC_CALENDAR_URL,
        notes=notes,
        activity_kind=_cdc_activity_kind(normalized_title),
        quality_flags=["text_source", *note_flags],
    )


def _cdc_make_text_only_event(event_date: date, text: str) -> dict[str, object] | None:
    normalized_title = normalize_space(text)
    if not normalized_title or any(city in normalized_title.lower() for city in ["prescott", "tucson"]):
        return None
    notes, note_flags = clean_event_notes("Parsed from CDC monthly calendar image.", max_length=120)
    return make_event(
        title=normalized_title,
        start_at=f"{event_date.isoformat()}T12:00:00-07:00",
        end_at=None,
        venue=CDC_DEFAULT_VENUE,
        city=CDC_DEFAULT_CITY,
        dance_style=_cdc_infer_style(normalized_title),
        source_name="CDC Studios",
        source_url=CDC_CALENDAR_URL,
        notes=notes,
        activity_kind=_cdc_activity_kind(normalized_title),
        quality_flags=["text_source", "fallback_time", *note_flags],
    )


def _cdc_infer_style(title: str) -> str:
    haystack = title.lower()
    if "country swing" in haystack:
        return "Country Swing"
    if "two-step" in haystack or "country" in haystack:
        return "Country"
    if "west coast" in haystack or "wcs" in haystack:
        return "West Coast Swing"
    if "bachata" in haystack:
        return "Bachata"
    if "salsa" in haystack:
        return "Salsa"
    if "hustle" in haystack:
        return "Hustle"
    if any(keyword in haystack for keyword in ["tango", "rumba", "waltz", "foxtrot", "cha cha", "mambo", "ecs"]):
        return "Ballroom"
    return "Social Dance"


def _cdc_activity_kind(title: str) -> str:
    lowered = title.lower()
    if any(keyword in lowered for keyword in ["social", "dance party", "practice session"]):
        return "Social"
    if "workshop" in lowered:
        return "Special Event"
    return "Lesson"


def _infer_swing_style(tags: str, title: str) -> str:
    haystack = f"{tags} {title}".lower()
    if "west coast" in haystack:
        return "West Coast Swing"
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
        "tango",
        "milonga",
        "practica",
        "zouk",
        "kizomba",
        "contra",
        "english country",
        "scottish country",
        "line dance",
        "country dance",
        "ballroom",
        "hustle",
        "west coast",
        "latin night",
        "social dance",
    ]
    blockers = ["dance games", "arcade", "power card", "improve dance style"]
    return any(keyword in haystack for keyword in keywords) and not any(blocker in haystack for blocker in blockers)


def _infer_dance_style(title: str, notes: str) -> str:
    haystack = f"{title} {notes}".lower()
    if "argentine tango" in haystack:
        return "Argentine Tango"
    if "tango" in haystack:
        return "Tango"
    if "english country" in haystack:
        return "English Country"
    if "scottish country" in haystack:
        return "Scottish Country"
    if "country swing" in haystack:
        return "Country Swing"
    if "brazilian zouk" in haystack or "zouk" in haystack:
        return "Brazilian Zouk"
    if "kizomba" in haystack or "urban kiz" in haystack:
        return "Kizomba"
    if "west coast" in haystack or "wcs" in haystack:
        return "West Coast Swing"
    if "salsa" in haystack and "bachata" in haystack:
        return "Salsa / Bachata"
    if "bachata" in haystack:
        return "Bachata"
    if "hustle" in haystack:
        return "Hustle"
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
    if cleaned.startswith("(") or cleaned.endswith(":"):
        return False
    if re.search(r"\b\d{1,2}(?::\d{2})?\s*(?:am|pm)\b", lower):
        return False
    if lower.startswith("price:") or "call for more info" in lower:
        return False
    if lower.startswith("time:") or lower.startswith("location:"):
        return False
    if any(marker in lower for marker in ["join today", "pay here", "testimonials", "find us on google!", "copyright"]):
        return False
    if infer_city(cleaned) or lower.startswith("http"):
        return False
    title_keywords = [
        "salsa",
        "bachata",
        "kizomba",
        "styling",
        "partnering",
        "footwork",
        "fusion",
        "bootcamp",
        "choreography",
        "team",
        "ladies",
    ]
    return _looks_like_dance_event(cleaned, "") or any(keyword in lower for keyword in title_keywords)


def _looks_like_location_line(value: str) -> bool:
    cleaned = decode_text(value, strip_markup=True)
    lower = cleaned.lower()
    if not cleaned or len(cleaned) > 120:
        return False
    if not (infer_city(cleaned) or "az" in lower or "arizona" in lower):
        return False
    if any(
        token in lower
        for token in ["review", "recommend", "professional", "atmosphere", "classes", "testimonials", "join today", "pay here", "copyright"]
    ):
        return False
    return (
        "," in cleaned
        or bool(re.search(r"\b\d{3,}\b", cleaned))
        or any(token in lower for token in ["ballroom", "dancewise", "studio", "center", "space", "club", "saloon", "district", "lounge"])
    )


def _clean_phoenix_salsa_notes(lines: list[str], *, title: str, location_line: str) -> str:
    cleaned_parts: list[str] = []
    ignored_fragments = {
        "Join Today!",
        "Pay Here",
        "Classes",
        "Testimonials",
        "Find us on Google!",
        "Copyright 2026",
    }
    ignored_markers = [
        "contact us",
        "text/call",
        "let's dance",
        "please fill out",
        "window.__",
        "__nuxt__",
        "function(",
        "leadconnector",
        "sourceurl=",
        "copyright 2026",
    ]
    for line in lines:
        if line in {title, location_line}:
            continue
        cleaned = decode_text(line, strip_markup=True)
        cleaned = re.sub(rf"\b{MONTH_PATTERN}\s+\d{{1,2}}(?:st|nd|rd|th)?\b", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(
            r"(?i)\btime:\s*\d{1,2}(?::\d{2})?\s*(?:am|pm)\s*-\s*\d{1,2}(?::\d{2})?\s*(?:am|pm)\b",
            "",
            cleaned,
        )
        cleaned = normalize_space(cleaned.strip(" -•"))
        if not cleaned:
            continue
        fragments = [normalize_space(part) for part in cleaned.split("•")]
        for fragment in fragments:
            lower = fragment.lower()
            if not fragment or fragment in ignored_fragments:
                continue
            if len(fragment) > 180:
                continue
            if any(marker in lower for marker in ignored_markers):
                continue
            if re.fullmatch(r"\d{3}[.\-]\d{3}[.\-]\d{4}", fragment):
                continue
            if fragment.startswith("{") or fragment.startswith("[") or fragment.startswith("/*") or fragment.startswith("function"):
                continue
            if fragment.startswith(""):
                continue
            if fragment not in ignored_fragments:
                cleaned_parts.append(fragment)
    deduped = list(dict.fromkeys(cleaned_parts))
    notes, _ = clean_event_notes(" • ".join(deduped[:8]))
    return notes


def _looks_like_partner_dance_event(title: str, notes: str) -> bool:
    if not _looks_like_dance_event(title, notes):
        return False
    haystack = f"{title} {notes}".lower()
    blockers = ["zumba", "fitness", "cardio", "hip hop", "workout", "exercise"]
    return not any(blocker in haystack for blocker in blockers)


def _fetch_google_calendar_source(
    fetch_text,
    today: date,
    *,
    source_name: str,
    page_url: str,
    default_style: str,
    fallback_city: str,
) -> list[dict[str, object]]:
    html = fetch_text(page_url)
    calendar_ids = _extract_google_calendar_ids(html)
    events: list[dict[str, object]] = []
    for calendar_id in calendar_ids:
        ics_text = fetch_text(_google_calendar_ics_url(calendar_id))
        for entry in parse_ics_events(ics_text):
            if entry.get("STATUS", "").upper() == "CANCELLED":
                continue
            summary = decode_text(entry.get("SUMMARY", ""), strip_markup=True)
            if not summary:
                continue
            description, note_flags = clean_event_notes(entry.get("DESCRIPTION", ""))
            location = decode_text(entry.get("LOCATION", ""), strip_markup=True)
            venue, city = split_venue_and_city(location)
            for occurrence_start, occurrence_end in expand_ics_occurrences(entry, today=today):
                events.append(
                    _make_google_calendar_event(
                        title=summary,
                        start_value=occurrence_start,
                        end_value=occurrence_end,
                        venue=venue or location,
                        city=city or infer_city(location, description) or fallback_city,
                        dance_style=default_style,
                        source_name=source_name,
                        source_url=decode_text(entry.get("URL", "")) or page_url,
                        notes=description,
                        quality_flags=["ics_source", "structured_source", *note_flags],
                    )
                )
    return deduplicate_events(events)


def _extract_google_calendar_ids(html: str) -> list[str]:
    calendar_ids: list[str] = []
    for raw_id in re.findall(r"https://calendar\.google\.com/calendar/embed\?src=([^\"'&<#]+)", html, re.IGNORECASE):
        calendar_id = unquote(unescape(raw_id).strip())
        if calendar_id and calendar_id not in calendar_ids:
            calendar_ids.append(calendar_id)
    return calendar_ids


def _google_calendar_ics_url(calendar_id: str) -> str:
    return f"https://calendar.google.com/calendar/ical/{quote(calendar_id, safe='')}/public/basic.ics"


def _make_google_calendar_event(
    *,
    title: str,
    start_value: datetime | date,
    end_value: datetime | date | None,
    venue: str,
    city: str,
    dance_style: str,
    source_name: str,
    source_url: str,
    notes: str,
    quality_flags: list[str],
) -> dict[str, object]:
    if isinstance(start_value, datetime):
        return make_event(
            title=title,
            start_at=serialize_dt(start_value),
            end_at=serialize_dt(end_value) if isinstance(end_value, datetime) else None,
            venue=venue,
            city=city,
            dance_style=dance_style,
            source_name=source_name,
            source_url=source_url,
            notes=notes,
            quality_flags=quality_flags,
        )

    end_at = f"{end_value.isoformat()}T00:00:00-07:00" if isinstance(end_value, date) else None
    return make_event(
        title=title,
        start_at=f"{start_value.isoformat()}T00:00:00-07:00",
        end_at=end_at,
        venue=venue,
        city=city,
        dance_style=dance_style,
        source_name=source_name,
        source_url=source_url,
        notes=notes,
        all_day=True,
        quality_flags=quality_flags,
    )


def _parse_bachata_addiction_specials(lines: list[str], today: date) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        title_match = re.search(r"\"([^\"]+)\"\s+(Saturday|Sunday|Monday|Tuesday|Wednesday|Thursday|Friday)\s+([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?!", line)
        if not title_match:
            continue
        month_name = title_match.group(3)
        day = int(title_match.group(4))
        event_date = datetime.strptime(f"{month_name} {day} {today.year}", "%B %d %Y").date()
        if event_date < today:
            continue
        chunk = " ".join(lines[index : index + 4])
        start_match = re.search(r"Lessons start at (\d{1,2})(?::(\d{2}))?\s*pm", chunk, re.IGNORECASE)
        dance_match = re.search(r"Dancing (\d{1,2})(?::(\d{2}))?-(\d{1,2})(?::(\d{2}))?am", chunk, re.IGNORECASE)
        if start_match:
            start_hour = int(start_match.group(1)) % 12 + 12
            start_minute = int(start_match.group(2) or "0")
            start_time = time(start_hour, start_minute)
        elif dance_match:
            start_hour = int(dance_match.group(1)) % 12 + 12
            start_minute = int(dance_match.group(2) or "0")
            start_time = time(start_hour, start_minute)
        else:
            continue
        end_time = None
        if dance_match:
            end_hour = int(dance_match.group(3)) % 12
            end_minute = int(dance_match.group(4) or "0")
            end_time = time(end_hour, end_minute)
        start_dt, end_dt = combine_event_range(event_date, start_time, end_time)
        notes, note_flags = clean_event_notes(chunk)
        events.append(
            make_event(
                title=title_match.group(1),
                start_at=serialize_dt(start_dt),
                end_at=serialize_dt(end_dt),
                venue="",
                city="Phoenix",
                dance_style="Bachata",
                source_name="Bachata Addiction",
                source_url=BACHATA_ADDICTION_URL,
                notes=notes,
                activity_kind="Special Event",
                quality_flags=["text_source", *note_flags],
            )
        )
    return events
