from __future__ import annotations

import hashlib
import re
from datetime import datetime

CITY_COORDINATES: dict[str, tuple[float, float]] = {
    "phoenix": (33.4484, -112.0740),
    "mesa": (33.4152, -111.8315),
    "scottsdale": (33.4942, -111.9261),
    "tempe": (33.4255, -111.9400),
    "chandler": (33.3062, -111.8413),
    "gilbert": (33.3528, -111.7890),
}
KNOWN_VENUES: tuple[dict[str, object], ...] = (
    {
        "aliases": ("scootin boots dance hall", "515 n stapley dr"),
        "venue": "Scootin Boots Dance Hall",
        "city": "Mesa",
        "latitude": 33.424332,
        "longitude": -111.806120,
    },
    {
        "aliases": ("phoenix salsa dance", "2530 n 7th st"),
        "venue": "Phoenix Salsa Dance",
        "city": "Phoenix",
        "latitude": 33.476113,
        "longitude": -112.064994,
    },
    {
        "aliases": ("fatcat ballroom", "3131 e thunderbird rd"),
        "venue": "Fatcat Ballroom",
        "city": "Phoenix",
        "latitude": 33.609569,
        "longitude": -112.014301,
    },
    {
        "aliases": (
            "scottsdale neighborhood arts place",
            "4425 n granite reef rd",
            "4425 n granite reef road",
        ),
        "venue": "Scottsdale Neighborhood Arts Place",
        "city": "Scottsdale",
        "latitude": 33.499564,
        "longitude": -111.899673,
    },
    {
        "aliases": ("dancewise", "5555 n 7th st"),
        "venue": "DanceWise",
        "city": "Phoenix",
        "latitude": 33.518243,
        "longitude": -112.064833,
    },
    {
        "aliases": ("nrg ballroom", "931 e elliot rd", "931 e elliot road"),
        "venue": "NRG Ballroom",
        "city": "Tempe",
        "latitude": 33.348366,
        "longitude": -111.926014,
    },
    {
        "aliases": ("z room", "1337 s gilbert rd", "1337 s gilbert road"),
        "venue": "Z Room | Dance + Filming",
        "city": "Mesa",
        "latitude": 33.390228,
        "longitude": -111.790796,
    },
    {
        "aliases": ("bethany lutheran church", "4300 n 82nd st"),
        "venue": "Bethany Lutheran Church",
        "city": "Scottsdale",
        "latitude": 33.498775,
        "longitude": -111.905820,
    },
    {
        "aliases": ("32 s center st", "heritage academy"),
        "venue": "Heritage Academy",
        "city": "Mesa",
        "latitude": 33.414391,
        "longitude": -111.831253,
    },
    {
        "aliases": ("1106 n central ave", "irish cultural center"),
        "venue": "Irish Cultural Center",
        "city": "Phoenix",
        "latitude": 33.460400,
        "longitude": -112.074000,
    },
    {
        "aliases": ("2716 n dobson rd", "greek orthodox church community center"),
        "venue": "Greek Orthodox Church Community Center",
        "city": "Chandler",
        "latitude": 33.344378,
        "longitude": -111.876056,
    },
    {
        "aliases": ("1316 e cheery lynn rd", "phoenix conservatory of music"),
        "venue": "Phoenix Conservatory of Music",
        "city": "Phoenix",
        "latitude": 33.483600,
        "longitude": -112.052800,
    },
    {
        "aliases": ("1905 e hackamore st",),
        "venue": "1905 E Hackamore St",
        "city": "Gilbert",
        "latitude": 33.382000,
        "longitude": -111.791500,
    },
    {
        "aliases": (
            "rscds phoenix branch",
            "granite reef senior center",
            "1700 n granite reef rd",
            "1700 n granite reef road",
        ),
        "canonicalize": True,
        "venue": "Granite Reef Senior Center, 1700 N Granite Reef Rd",
        "city": "Scottsdale",
        "latitude": 33.4670947,
        "longitude": -111.9016525,
    },
    {
        "aliases": ("guild of the vale", "200 n macdonald"),
        "canonicalize": True,
        "venue": "Guild of the Vale, 200 N Macdonald",
        "city": "Mesa",
        "latitude": 33.4195920,
        "longitude": -111.8341560,
    },
    {
        "aliases": ("the cove swing club", "cove swing club", "2240 w desert cove ave"),
        "canonicalize": True,
        "venue": "The Cove Swing Club, 2240 W Desert Cove Ave",
        "city": "Phoenix",
        "latitude": 33.5858521,
        "longitude": -112.1061979,
    },
    {
        "aliases": ("the duce", "525 s central ave"),
        "canonicalize": True,
        "venue": "The Duce, 525 S Central Ave",
        "city": "Phoenix",
        "latitude": 33.4423306,
        "longitude": -112.0736119,
    },
    {
        "aliases": ("spellbound studios", "4902 e mcdowell rd"),
        "canonicalize": True,
        "venue": "Spellbound Studios, 4902 E McDowell Rd",
        "city": "Phoenix",
        "latitude": 33.4661462,
        "longitude": -111.9759423,
    },
    {
        "aliases": ("versalles reception hall", "1422 e main st"),
        "canonicalize": True,
        "venue": "Versalles Reception Hall, 1422 E Main St",
        "city": "Mesa",
        "latitude": 33.4153140,
        "longitude": -111.8021990,
    },
)

NEGATED_CANCELLATION_PATTERN = re.compile(
    r"\b(?:not|is not|isn't|has not been|hasn't been|never)\b.{0,24}\bcancel(?:led|ed)\b",
    re.IGNORECASE,
)

NON_WARNING_QUALITY_FLAGS = {
    "ics_source",
    "manual_source",
    "recurring_source",
    "structured_source",
    "text_source",
    "inferred_city",
    "notes_truncated",
    "sanitized_markup",
}
SUSPICIOUS_QUALITY_FLAGS = {
    "fallback_location",
    "fallback_time",
    "missing_city",
    "missing_venue",
    "script_noise_removed",
    "source_degraded",
}


def normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_location_token(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", normalize_space(value).lower()).strip()


def canonicalize_location(venue: str | None, city: str | None) -> tuple[str, str]:
    normalized_venue = normalize_location_token(venue)
    for details in KNOWN_VENUES:
        aliases = details["aliases"]
        if any(alias in normalized_venue for alias in aliases) and details.get("canonicalize"):  # type: ignore[arg-type]
            return str(details["venue"]), str(details["city"])
    return normalize_space(venue), normalize_space(city)


def resolve_location(venue: str | None, city: str | None) -> dict[str, object]:
    normalized_venue = normalize_location_token(venue)
    normalized_city = normalize_location_token(city)

    for details in KNOWN_VENUES:
        aliases = details["aliases"]
        if any(alias in normalized_venue for alias in aliases):  # type: ignore[arg-type]
            return {
                "latitude": details["latitude"],
                "longitude": details["longitude"],
                "location_precision": "venue",
            }

    if normalized_city in CITY_COORDINATES:
        latitude, longitude = CITY_COORDINATES[normalized_city]
        return {
            "latitude": latitude,
            "longitude": longitude,
            "location_precision": "city",
        }

    return {
        "latitude": None,
        "longitude": None,
        "location_precision": "",
    }


def text_mentions_cancellation(value: str | None) -> bool:
    cleaned = normalize_space(value)
    if "cancel" not in cleaned.lower():
        return False
    for sentence in re.split(r"[.!?]+", cleaned):
        candidate = normalize_space(sentence)
        if "cancel" not in candidate.lower():
            continue
        if NEGATED_CANCELLATION_PATTERN.search(candidate):
            continue
        if re.search(r"\bcancel(?:led|ed|lation)\b", candidate, re.IGNORECASE):
            return True
    return False


def is_cancelled_event(event: dict[str, object]) -> bool:
    return text_mentions_cancellation(str(event.get("title", ""))) or text_mentions_cancellation(str(event.get("notes", "")))


def normalize_quality_flags(flags: list[str] | None) -> list[str]:
    unique: list[str] = []
    for flag in flags or []:
        cleaned = normalize_space(flag)
        if cleaned and cleaned not in unique:
            unique.append(cleaned)
    return sorted(unique)


def quality_note_for_flags(flags: list[str] | None) -> str | None:
    normalized = normalize_quality_flags(flags)
    if any(flag in SUSPICIOUS_QUALITY_FLAGS for flag in normalized):
        return "Details may be incomplete; check the original source."
    if any(flag not in NON_WARNING_QUALITY_FLAGS for flag in normalized):
        return "Details may be incomplete; check the original source."
    return None


def build_event_id(source_name: str, title: str, start_at: str, venue: str, city: str) -> str:
    seed = "|".join(
        [
            normalize_space(source_name).lower(),
            normalize_space(title).lower(),
            start_at,
            normalize_space(venue).lower(),
            normalize_space(city).lower(),
        ]
    )
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def infer_activity_kind(title: str, notes: str = "") -> str:
    haystack = normalize_space(f"{title} {notes}").lower()

    special_keywords = [
        "festival",
        "weekender",
        "weekend",
        "workshop",
        "intensive",
        "congress",
        "camp",
        "retreat",
        "bootcamp",
        "special event",
    ]
    social_keywords = [
        "social",
        "dance",
        "party",
        "night",
        "milonga",
        "practica",
        "live band",
        "open dance",
        "social dancing",
    ]
    lesson_keywords = [
        "lesson",
        "class",
        "styling",
        "partnering",
        "drill",
        "practice",
        "training",
    ]

    if any(keyword in haystack for keyword in special_keywords):
        return "Special Event"
    if any(keyword in haystack for keyword in social_keywords):
        return "Social"
    if any(keyword in haystack for keyword in lesson_keywords):
        return "Lesson"
    return "Social"


def make_event(
    *,
    title: str,
    start_at: str,
    end_at: str | None,
    venue: str,
    city: str,
    dance_style: str,
    source_name: str,
    source_url: str,
    notes: str = "",
    activity_kind: str | None = None,
    all_day: bool = False,
    last_seen_at: str | None = None,
    event_id: str | None = None,
    quality_flags: list[str] | None = None,
    quality_note: str | None = None,
) -> dict[str, object]:
    canonical_venue, canonical_city = canonicalize_location(venue, city)
    location = resolve_location(canonical_venue, canonical_city)
    normalized_quality_flags = normalize_quality_flags(
        [
            *(quality_flags or []),
            *([] if canonical_venue else ["missing_venue"]),
            *([] if canonical_city else ["missing_city"]),
        ]
    )
    event = {
        "id": event_id or build_event_id(source_name, title, start_at, canonical_venue, canonical_city),
        "title": normalize_space(title),
        "start_at": start_at,
        "end_at": end_at,
        "all_day": all_day,
        "venue": canonical_venue,
        "city": canonical_city,
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "location_precision": location["location_precision"],
        "dance_style": normalize_space(dance_style),
        "activity_kind": normalize_space(activity_kind) or infer_activity_kind(title, notes),
        "source_name": normalize_space(source_name),
        "source_url": source_url,
        "notes": normalize_space(notes),
        "last_seen_at": last_seen_at or datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "quality_flags": normalized_quality_flags,
        "quality_note": normalize_space(quality_note) or quality_note_for_flags(normalized_quality_flags),
    }
    return event
