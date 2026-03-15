from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

CITY_COORDINATES: dict[str, tuple[float, float]] = {
    "phoenix": (33.4484, -112.0740),
    "mesa": (33.4152, -111.8315),
    "scottsdale": (33.4942, -111.9261),
    "tempe": (33.4255, -111.9400),
    "chandler": (33.3062, -111.8413),
    "gilbert": (33.3528, -111.7890),
}
KNOWN_VENUES_PATH = Path(__file__).with_name("known_venues.json")

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


def load_known_venues() -> tuple[dict[str, object], ...]:
    payload = json.loads(KNOWN_VENUES_PATH.read_text())
    venues: list[dict[str, object]] = []
    for entry in payload:
        aliases = tuple(
            normalize_location_token(alias)
            for alias in entry.get("aliases", [])
            if normalize_location_token(alias)
        )
        venues.append(
            {
                "aliases": aliases,
                "canonicalize": bool(entry.get("canonicalize")),
                "venue": normalize_space(entry.get("venue")),
                "city": normalize_space(entry.get("city")),
                "latitude": entry.get("latitude"),
                "longitude": entry.get("longitude"),
            }
        )
    return tuple(venues)


KNOWN_VENUES = load_known_venues()


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
