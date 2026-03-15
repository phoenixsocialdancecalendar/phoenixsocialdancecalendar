from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from unittest import TestCase

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from dance_calendar.parsing import deduplicate_events
from dance_calendar.models import infer_activity_kind
from dance_calendar.sources import (
    DAVE_AND_BUSTERS_TEMPE_URL,
    PHOENIX_SALSA_DANCE_URL,
    PHXTMD_URL,
    SALSA_VIDA_CALENDAR_URL,
    SWING_DANCING_PHOENIX_API,
    GREATER_PHOENIX_SWING_URL,
    fetch_dave_and_busters_tempe,
    fetch_greater_phoenix_swing,
    fetch_phoenix_salsa_dance,
    fetch_phxtmd,
    fetch_salsa_vida,
    fetch_swing_dancing_phoenix,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def read_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text()


class SourceTests(TestCase):
    def test_activity_kind_counts_lesson_before_social_as_social(self) -> None:
        self.assertEqual(
            infer_activity_kind("Beginner lesson + social dancing", "Lesson at 7, dance at 8."),
            "Social",
        )

    def test_activity_kind_marks_workshops_as_special_events(self) -> None:
        self.assertEqual(
            infer_activity_kind("Bachata Weekend Workshop", "Special guest instructors."),
            "Special Event",
        )

    def test_swing_source_uses_structured_api(self) -> None:
        today = date(2026, 3, 14)
        events = fetch_swing_dancing_phoenix(lambda url: read_fixture("swing_api.json"), today)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["city"], "Phoenix")
        self.assertEqual(events[0]["dance_style"], "Lindy Hop")

    def test_salsa_vida_follows_detail_links(self) -> None:
        today = date(2026, 3, 14)
        payloads = {
            SALSA_VIDA_CALENDAR_URL: read_fixture("salsa_vida_calendar.html"),
            "https://www.salsavida.com/event/arizona/phoenix/phoenix-salsa-social/": read_fixture("salsa_vida_event.html"),
            "https://www.salsavida.com/event/arizona/phoenix/phoenix-bachata-night/": read_fixture("salsa_vida_event.html")
            .replace("Phoenix Salsa Social", "Phoenix Bachata Night")
            .replace("Live DJ and salsa social in Phoenix.", "Late-night bachata social in Phoenix."),
        }

        events = fetch_salsa_vida(lambda url: payloads[url], today)

        self.assertEqual(len(events), 2)
        self.assertEqual({event["dance_style"] for event in events}, {"Salsa", "Bachata"})

    def test_phxtmd_deduplicates_repeated_blocks(self) -> None:
        events = fetch_phxtmd(lambda url: read_fixture("phxtmd.html"), date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["dance_style"], "Contra")

    def test_recurring_sources_expand_upcoming_occurrences(self) -> None:
        today = date(2026, 3, 14)
        swing_events = fetch_greater_phoenix_swing(lambda url: read_fixture("greater_phoenix_swing.html"), today)
        salsa_events = fetch_phoenix_salsa_dance(lambda url: read_fixture("phoenix_salsa_dance.html"), today)

        self.assertGreaterEqual(len(swing_events), 4)
        self.assertGreaterEqual(len(salsa_events), 8)
        self.assertIn("Scottsdale", {event["city"] for event in salsa_events})

    def test_phoenix_salsa_dance_skips_price_lines_as_titles(self) -> None:
        html = """
        <html>
          <body>
            <h2>Saturday</h2>
            <p>Price: $15</p>
            <p>• Improve dance style</p>
            <p>Ladies Salsa Styling</p>
            <p>11:00 AM - 12:00 PM</p>
            <p>Scottsdale, AZ</p>
          </body>
        </html>
        """

        events = fetch_phoenix_salsa_dance(lambda url: html, date(2026, 3, 14))

        self.assertEqual(events[0]["title"], "Ladies Salsa Styling")

    def test_deduplicate_events_merges_same_start_title_and_place(self) -> None:
        events = [
            {
                "id": "one",
                "title": "Friday Night Swing Social",
                "start_at": "2026-03-20T19:15:00-07:00",
                "end_at": "2026-03-20T22:30:00-07:00",
                "all_day": False,
                "venue": "The Cove Swing Club",
                "city": "Phoenix",
                "dance_style": "Swing",
                "source_name": "Source One",
                "source_url": "https://one.example",
                "notes": "Lesson at 7:15.",
                "last_seen_at": "2026-03-14T00:00:00Z",
            },
            {
                "id": "two",
                "title": "Friday Night Swing Social at The Cove",
                "start_at": "2026-03-20T19:15:00-07:00",
                "end_at": "2026-03-20T22:30:00-07:00",
                "all_day": False,
                "venue": "Cove Swing Club",
                "city": "Phoenix",
                "dance_style": "Swing",
                "source_name": "Source Two",
                "source_url": "https://two.example",
                "notes": "Social dancing.",
                "last_seen_at": "2026-03-14T00:00:00Z",
            },
        ]

        deduped = deduplicate_events(events)

        self.assertEqual(len(deduped), 1)
        self.assertIn("Source Two", deduped[0]["notes"])

    def test_dave_and_busters_ignores_non_event_dance_mentions(self) -> None:
        html = """
        <html>
          <body>
            <h2>Special Events</h2>
            <p>Enjoy arcade classics, dance games, and happy hour specials.</p>
          </body>
        </html>
        """

        events = fetch_dave_and_busters_tempe(lambda url: html, date(2026, 3, 14))

        self.assertEqual(events, [])
