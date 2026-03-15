from __future__ import annotations

import hashlib
import re
from datetime import datetime


def normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


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
) -> dict[str, object]:
    event = {
        "id": event_id or build_event_id(source_name, title, start_at, venue, city),
        "title": normalize_space(title),
        "start_at": start_at,
        "end_at": end_at,
        "all_day": all_day,
        "venue": normalize_space(venue),
        "city": normalize_space(city),
        "dance_style": normalize_space(dance_style),
        "activity_kind": normalize_space(activity_kind) or infer_activity_kind(title, notes),
        "source_name": normalize_space(source_name),
        "source_url": source_url,
        "notes": normalize_space(notes),
        "last_seen_at": last_seen_at or datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    return event
