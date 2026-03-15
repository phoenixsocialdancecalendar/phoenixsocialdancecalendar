from __future__ import annotations

import json
import sys
import tempfile
from datetime import date
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from dance_calendar.models import infer_activity_kind, make_event, resolve_location
from dance_calendar.parsing import deduplicate_events, extract_text_lines
from dance_calendar.pipeline import (
    build_event_catalog_with_report,
    exclude_non_social_dance_events,
    run_source,
    suppress_cdc_ocr_duplicates,
)
from dance_calendar.sources import (
    DAVE_AND_BUSTERS_TEMPE_URL,
    BACHATA_ADDICTION_URL,
    AZSALSA_TUMBAO_URL,
    DESERT_CITY_SWING_URL,
    FATCAT_ARGENTINE_TANGO_URL,
    FATCAT_LINE_DANCING_URL,
    FATCAT_MIDWEEK_BALLROOM_URL,
    FATCAT_MONDAY_SMOOTH_URL,
    FATCAT_SALSA_URL,
    FATCAT_TRIPLE_STEP_URL,
    FATCAT_WEST_COAST_SWING_URL,
    ENGLISH_COUNTRY_URL,
    PHOENIX_SALSA_DANCE_URL,
    PHOENIX_ARGENTINE_TANGO_URL,
    PHXTMD_URL,
    PHXTMD_ENGLISH_URL,
    PHXTMD_SPECIAL_URL,
    NRG_BALLROOM_MONTH_URL,
    HAROLDS_CORRAL_API_URL,
    SALSA_VIDA_CALENDAR_URL,
    SCOOTIN_BOOTS_URL,
    SWING_DANCING_PHOENIX_API,
    GREATER_PHOENIX_SWING_URL,
    CDC_CALENDAR_URL,
    LATIN_SOL_URL,
    SUMMER_SWING_FEST_URL,
    SWINGDEPENDANCE_URL,
    WHITE_RABBIT_WCS_URL,
    ZOOK_PHOENIX_URL,
    SourceDefinition,
    fetch_azsalsa_tumbao,
    fetch_bachata_addiction,
    fetch_cdc_calendar,
    fetch_desert_city_swing,
    fetch_dancewise,
    fetch_dave_and_busters_tempe,
    fetch_english_country,
    fetch_fatcat_ballroom,
    fetch_fatcat_meetup,
    fetch_greater_phoenix_swing,
    fetch_phoenix_4th,
    fetch_phoenix_argentine_tango,
    fetch_phoenix_salsa_dance,
    fetch_phxtmd,
    fetch_phxtmd_special_events,
    fetch_rscds_phoenix,
    fetch_salsa_vida,
    fetch_shall_we_dance,
    fetch_scootin_boots,
    fetch_summer_swing_fest,
    fetch_swingdependance,
    fetch_swing_dancing_phoenix,
    fetch_white_rabbit_wcs,
    fetch_latin_sol,
    fetch_harolds_corral,
    fetch_nrg_ballroom,
    fetch_zouk_phoenix,
    _infer_dance_style,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def read_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text()


class SourceTests(TestCase):
    def test_resolve_location_prefers_known_venue_coordinates(self) -> None:
        location = resolve_location("Fatcat Ballroom", "Phoenix")

        self.assertEqual(location["location_precision"], "venue")
        self.assertAlmostEqual(float(location["latitude"]), 33.609569, places=5)
        self.assertAlmostEqual(float(location["longitude"]), -112.014301, places=5)

    def test_resolve_location_supports_verified_alias_locations(self) -> None:
        location = resolve_location("RSCDS Phoenix Branch", "Phoenix")

        self.assertEqual(location["location_precision"], "venue")
        self.assertAlmostEqual(float(location["latitude"]), 33.4670947, places=5)
        self.assertAlmostEqual(float(location["longitude"]), -111.9016525, places=5)

    def test_resolve_location_falls_back_to_city_coordinates(self) -> None:
        location = resolve_location("Unknown Ballroom", "Mesa")

        self.assertEqual(location["location_precision"], "city")
        self.assertAlmostEqual(float(location["latitude"]), 33.4152, places=4)
        self.assertAlmostEqual(float(location["longitude"]), -111.8315, places=4)

    def test_make_event_includes_location_metadata(self) -> None:
        event = make_event(
            title="Map Test Social",
            start_at="2026-03-20T19:00:00-07:00",
            end_at="2026-03-20T22:00:00-07:00",
            venue="Phoenix Salsa Dance",
            city="Phoenix",
            dance_style="Salsa",
            source_name="Map Source",
            source_url="https://example.com/map",
        )

        self.assertEqual(event["location_precision"], "venue")
        self.assertIsInstance(event["latitude"], float)
        self.assertIsInstance(event["longitude"], float)

    def test_make_event_canonicalizes_verified_venue_aliases(self) -> None:
        event = make_event(
            title="Scottish Country Dance Class",
            start_at="2026-03-17T18:30:00-07:00",
            end_at="2026-03-17T20:00:00-07:00",
            venue="RSCDS Phoenix Branch",
            city="Phoenix",
            dance_style="Scottish Country",
            source_name="RSCDS Phoenix Branch",
            source_url="https://www.rscdsphoenix.com/p/classes-2.html",
        )

        self.assertEqual(event["venue"], "Granite Reef Senior Center, 1700 N Granite Reef Rd")
        self.assertEqual(event["city"], "Scottsdale")
        self.assertEqual(event["location_precision"], "venue")

    def test_infer_dance_style_treats_country_swing_as_country_style(self) -> None:
        self.assertEqual(
            _infer_dance_style("Country Swing Social", "Beginner-friendly partner dancing all night."),
            "Country Swing",
        )

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
        self.assertEqual(events[0]["venue"], "The Cove Swing Club, 2240 W Desert Cove Ave")
        self.assertEqual(events[0]["city"], "Phoenix")
        self.assertEqual(events[0]["location_precision"], "venue")
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

    def test_phoenix_argentine_tango_expands_google_calendar_recurring_events(self) -> None:
        today = date(2026, 3, 14)
        html = """
        <html>
          <body>
            <iframe src="https://calendar.google.com/calendar/embed?src=milongas%40group.calendar.google.com&amp;ctz=America%2FPhoenix"></iframe>
            <iframe src="https://calendar.google.com/calendar/embed?src=classes%40group.calendar.google.com&amp;ctz=America%2FPhoenix"></iframe>
          </body>
        </html>
        """
        milongas_ics = """
BEGIN:VCALENDAR
BEGIN:VEVENT
DTSTART;TZID=America/Phoenix:20260313T210000
DTEND;TZID=America/Phoenix:20260314T000000
RRULE:FREQ=MONTHLY;UNTIL=20260613T065959Z;BYDAY=2FR
SUMMARY:La Dolce Vita Milonga
LOCATION:DanceWise Dance Studio, 5555 N 7th St Suite 112, Phoenix, AZ 85014, USA
DESCRIPTION:Monthly tango social.
END:VEVENT
END:VCALENDAR
"""
        classes_ics = """
BEGIN:VCALENDAR
BEGIN:VEVENT
DTSTART;TZID=America/Phoenix:20260311T190000
DTEND;TZID=America/Phoenix:20260311T203000
RRULE:FREQ=WEEKLY;UNTIL=20260430T065959Z;BYDAY=WE
EXDATE;TZID=America/Phoenix:20260318T190000
SUMMARY:Wednesday Practica
LOCATION:Scottsdale Neighborhood Arts Place, 4425 N Granite Reef Rd, Scottsdale, AZ 85251, USA
DESCRIPTION:Weekly tango practica.
END:VEVENT
END:VCALENDAR
"""
        payloads = {
            PHOENIX_ARGENTINE_TANGO_URL: html,
            "https://calendar.google.com/calendar/ical/milongas%40group.calendar.google.com/public/basic.ics": milongas_ics,
            "https://calendar.google.com/calendar/ical/classes%40group.calendar.google.com/public/basic.ics": classes_ics,
        }

        events = fetch_phoenix_argentine_tango(lambda url: payloads[url], today)

        self.assertEqual(len(events), 9)
        self.assertEqual({event["dance_style"] for event in events}, {"Argentine Tango"})
        self.assertIn("2026-04-10T21:00:00-07:00", {event["start_at"] for event in events})
        self.assertNotIn("2026-03-18T19:00:00-07:00", {event["start_at"] for event in events})
        self.assertIn("Scottsdale", {event["city"] for event in events})

    def test_phoenix_argentine_tango_respects_count_limited_past_courses(self) -> None:
        today = date(2026, 3, 14)
        html = """
        <html>
          <body>
            <iframe src="https://calendar.google.com/calendar/embed?src=classes%40group.calendar.google.com&amp;ctz=America%2FPhoenix"></iframe>
          </body>
        </html>
        """
        ics = """
BEGIN:VCALENDAR
BEGIN:VEVENT
DTSTART;TZID=America/Phoenix:20240311T183000
DTEND;TZID=America/Phoenix:20240311T200000
RRULE:FREQ=WEEKLY;COUNT=8;BYDAY=MO
SUMMARY:Beginner Argentine Tango 1
LOCATION:Scottsdale Neighborhood Arts Place, 4425 N Granite Reef Rd, Scottsdale, AZ 85251, USA
DESCRIPTION:An older 8-week course.
END:VEVENT
END:VCALENDAR
"""
        payloads = {
            PHOENIX_ARGENTINE_TANGO_URL: html,
            "https://calendar.google.com/calendar/ical/classes%40group.calendar.google.com/public/basic.ics": ics,
        }

        events = fetch_phoenix_argentine_tango(lambda url: payloads[url], today)

        self.assertEqual(events, [])

    def test_zouk_phoenix_expands_embedded_google_calendar(self) -> None:
        today = date(2026, 3, 14)
        html = """
        <html>
          <body>
            <iframe src="https://calendar.google.com/calendar/embed?src=zoukphoenix%40group.calendar.google.com&amp;ctz=America%2FPhoenix"></iframe>
          </body>
        </html>
        """
        ics = """
BEGIN:VCALENDAR
BEGIN:VEVENT
DTSTART;TZID=America/Phoenix:20260311T190000
DTEND;TZID=America/Phoenix:20260311T200000
RRULE:FREQ=WEEKLY;UNTIL=20260409T065959Z;BYDAY=WE
SUMMARY:Brazilian Zouk Conditioning and Practica
LOCATION:Z Room | Dance + Filming, 1337 S Gilbert Rd UNIT 116, Mesa, AZ 85204, USA
DESCRIPTION:Weekly all-levels zouk training and practica.
END:VEVENT
BEGIN:VEVENT
DTSTART:20260329T023000Z
DTEND:20260329T060000Z
SUMMARY:Zouk Dance Party
LOCATION:Scottsdale Neighborhood Arts Place, 4425 N Granite Reef Rd, Scottsdale, AZ 85251, USA
DESCRIPTION:Special social dance party.
END:VEVENT
END:VCALENDAR
"""
        payloads = {
            ZOOK_PHOENIX_URL: html,
            "https://calendar.google.com/calendar/ical/zoukphoenix%40group.calendar.google.com/public/basic.ics": ics,
        }

        events = fetch_zouk_phoenix(lambda url: payloads[url], today)

        self.assertEqual(len(events), 5)
        self.assertEqual({event["dance_style"] for event in events}, {"Brazilian Zouk"})
        self.assertIn("Mesa", {event["city"] for event in events})
        self.assertIn("2026-03-28T19:30:00-07:00", {event["start_at"] for event in events})

    def test_phxtmd_deduplicates_repeated_blocks(self) -> None:
        events = fetch_phxtmd(lambda url: read_fixture("phxtmd.html"), date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["dance_style"], "Contra")

    def test_phxtmd_parses_numeric_date_cards_and_alias_locations(self) -> None:
        html = """
        <html>
          <body>
            <p>ICC (map) = Irish Cultural Center Great Hall, 1106 N Central Av, Phoenix 85004</p>
            <p>GOCCC (map) = Greek Orthodox Church Community Center, 2716 N Dobson Rd, Chandler 85224</p>
            <p>PCM (map) = 1316 E Cheery Lynn Rd, Phoenix 85014</p>
            <p>03/27/26</p>
            <p>Fourth Friday (new location • this month only)</p>
            <p>7 PM</p>
            <p>-</p>
            <p>10 PM</p>
            <p>Phoenix Conservatory of Music (PCM)</p>
            <p>Event Details</p>
            <p>03/27/26</p>
            <p>Fourth Friday (new location • this month only)</p>
            <p>Band: BIG FUN</p>
            <p>Caller: Paige Huston</p>
            <p>(Free lesson at 6:30 pm)</p>
            <p>SLIDING-SCALE ADMISSION</p>
            <p>Pay what you can afford</p>
            <p>7 PM</p>
            <p>-</p>
            <p>10 PM</p>
            <p>Phoenix Conservatory of Music (PCM)</p>
          </body>
        </html>
        """

        events = fetch_phxtmd(lambda url: html, date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["start_at"], "2026-03-27T19:00:00-07:00")
        self.assertEqual(events[0]["end_at"], "2026-03-27T22:00:00-07:00")
        self.assertEqual(events[0]["city"], "Phoenix")
        self.assertIn("Phoenix Conservatory of Music", events[0]["venue"])
        self.assertIn("Band: BIG FUN", events[0]["notes"])

    def test_english_country_prefers_phxtmd_english_cards(self) -> None:
        phxtmd_html = """
        <html>
          <body>
            <p>ICC =Irish Cultural Center, 1106 N Central Ave, Phoenix 85004</p>
            <p>03/28/26</p>
            <p>ECD</p>
            <p>9 AM</p>
            <p>-</p>
            <p>11 AM</p>
            <p>ICC Great Hall</p>
            <p>Event Details</p>
            <p>03/28/26</p>
            <p>ECD</p>
            <p>Music: Recorded</p>
            <p>2nd & 4th Saturdays</p>
            <p>$10 at the door or Free Will Donation</p>
            <p>9 AM</p>
            <p>-</p>
            <p>11 AM</p>
            <p>ICC Great Hall</p>
            <p>04/28/26</p>
            <p>ECD</p>
            <p>9 AM</p>
            <p>-</p>
            <p>11 AM</p>
            <p>ICC Great Hall</p>
            <p>Event Details</p>
            <p>04/28/26</p>
            <p>ECD</p>
            <p>Music: Recorded</p>
            <p>2nd & 4th Saturdays</p>
            <p>$10 at the door or Free Will Donation</p>
            <p>9 AM</p>
            <p>-</p>
            <p>11 AM</p>
            <p>ICC Great Hall</p>
          </body>
        </html>
        """
        payloads = {
            PHXTMD_ENGLISH_URL: phxtmd_html,
            ENGLISH_COUNTRY_URL: read_fixture("english_country.html"),
        }

        events = fetch_english_country(lambda url: payloads[url], date(2026, 3, 14))

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["title"], "English Country Dancing")
        self.assertEqual(events[0]["source_url"], PHXTMD_ENGLISH_URL)
        self.assertEqual(events[0]["start_at"], "2026-03-28T09:00:00-07:00")
        self.assertEqual(events[1]["start_at"], "2026-04-25T09:00:00-07:00")
        self.assertTrue(all("Irish Cultural Center" in event["venue"] for event in events))

    def test_phxtmd_special_events_handles_empty_page(self) -> None:
        html = """
        <html>
          <body>
            <p>Special Events Calendar</p>
            <p>No upcoming events.</p>
          </body>
        </html>
        """

        events = fetch_phxtmd_special_events(lambda url: html, date(2026, 3, 14))

        self.assertEqual(events, [])

    def test_phxtmd_special_events_parses_future_cards(self) -> None:
        html = """
        <html>
          <body>
            <p>ICC (map) = Irish Cultural Center Great Hall, 1106 N Central Av, Phoenix 85004</p>
            <p>06/20/26</p>
            <p>Summer Solstice Dance Party</p>
            <p>7 PM</p>
            <p>-</p>
            <p>10 PM</p>
            <p>ICC Great Hall</p>
            <p>Event Details</p>
            <p>06/20/26</p>
            <p>Summer Solstice Dance Party</p>
            <p>Live band and community potluck</p>
            <p>Contra and English favorites all night</p>
            <p>7 PM</p>
            <p>-</p>
            <p>10 PM</p>
            <p>ICC Great Hall</p>
          </body>
        </html>
        """

        events = fetch_phxtmd_special_events(lambda url: html, date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["title"], "Summer Solstice Dance Party")
        self.assertEqual(events[0]["source_url"], PHXTMD_SPECIAL_URL)
        self.assertEqual(events[0]["activity_kind"], "Special Event")
        self.assertEqual(events[0]["start_at"], "2026-06-20T19:00:00-07:00")
        self.assertIn("Irish Cultural Center", events[0]["venue"])
        self.assertIn("Live band", events[0]["notes"])

    def test_extract_text_lines_ignores_script_style_and_noscript_content(self) -> None:
        html = """
        <html>
          <body>
            <script>window.__NUXT__ = { giant: "payload" };</script>
            <style>.hidden { display: none; }</style>
            <noscript>Fallback markup that should be ignored.</noscript>
            <p>Real event copy</p>
          </body>
        </html>
        """

        lines = extract_text_lines(html)

        self.assertEqual(lines, ["Real event copy"])

    def test_recurring_sources_expand_upcoming_occurrences(self) -> None:
        today = date(2026, 3, 14)
        swing_events = fetch_greater_phoenix_swing(lambda url: read_fixture("greater_phoenix_swing.html"), today)
        salsa_events = fetch_phoenix_salsa_dance(lambda url: read_fixture("phoenix_salsa_dance.html"), today)

        self.assertGreaterEqual(len(swing_events), 4)
        self.assertGreaterEqual(len(salsa_events), 8)
        self.assertIn("Scottsdale", {event["city"] for event in salsa_events})

    def test_fatcat_ballroom_adds_weekly_series_from_class_pages(self) -> None:
        today = date(2026, 3, 14)
        payloads = {
            FATCAT_SALSA_URL: "<html><body><p>Every Sunday 6PM</p><p>6:00 PM</p><p>8:00 PM</p></body></html>",
            FATCAT_ARGENTINE_TANGO_URL: "<html><body><p>5:30PM every Monday</p><p>5:30 PM</p></body></html>",
            FATCAT_MONDAY_SMOOTH_URL: "<html><body><p>Every Monday night</p><p>6:00–6:45 pm</p><p>7:30–8:00 pm</p></body></html>",
            FATCAT_TRIPLE_STEP_URL: "<html><body><p>Every Tuesday 7PM</p><p>7:00 – 8:00 PM</p><p>8:00 – 9:30 PM</p></body></html>",
            FATCAT_MIDWEEK_BALLROOM_URL: "<html><body><p>6:30PM every Wednesday</p><p>6:30 PM</p><p>8:00 PM</p></body></html>",
            FATCAT_LINE_DANCING_URL: "<html><body><p>6PM every Friday</p><p>6:00 PM</p></body></html>",
            FATCAT_WEST_COAST_SWING_URL: "<html><body><p>Every Friday 7PM</p><p>7:00 PM</p><p>8:30 PM</p></body></html>",
        }

        events = fetch_fatcat_ballroom(lambda url: payloads[url], today)

        self.assertEqual(len(events), 56)
        self.assertIn("Fatcat Ballroom", {event["source_name"] for event in events})
        self.assertIn("Argentine Tango", {event["dance_style"] for event in events})
        self.assertIn("West Coast Swing", {event["dance_style"] for event in events})
        self.assertIn("2026-03-20T19:00:00-07:00", {event["start_at"] for event in events})

    def test_phoenix_salsa_dance_splits_weekday_blocks_cleanly(self) -> None:
        events = fetch_phoenix_salsa_dance(lambda url: read_fixture("phoenix_salsa_dance.html"), date(2026, 3, 14))

        self.assertEqual(events[0]["venue"], "Dancewise")
        self.assertEqual(events[0]["city"], "Scottsdale")
        self.assertNotIn("Wednesday", events[0]["notes"])

    def test_phoenix_salsa_dance_strips_stale_dates_from_notes(self) -> None:
        html = """
        <html>
          <body>
            <h2>Saturday</h2>
            <p>Ladies Salsa Styling</p>
            <p>(Open Level) July 16th Time: 1PM- 3PM Price: $20 • Arm Styling</p>
            <p>Dancewise, Scottsdale, AZ</p>
          </body>
        </html>
        """

        events = fetch_phoenix_salsa_dance(lambda url: html, date(2026, 3, 14))

        self.assertEqual(events[0]["venue"], "Dancewise")
        self.assertEqual(events[0]["city"], "Scottsdale")
        self.assertEqual(events[0]["notes"], "(Open Level) Price: $20 • Arm Styling")

    def test_phoenix_salsa_dance_prefers_embedded_widget_events(self) -> None:
        calendar_html = """
        <html>
          <body>
            <p>Location:</p>
            <p>Phoenix Salsa Dance</p>
            <p>2530 N 7th St #107</p>
            <p>Phoenix, AZ 85006</p>
            <script src="https://apps.elfsight.com/p/platform.js" defer></script>
            <div class="elfsight-app-ec2244a5-766f-4f93-883c-de4e5d86e942"></div>
          </body>
        </html>
        """
        boot_payload = {
            "data": {
                "widgets": {
                    "ec2244a5-766f-4f93-883c-de4e5d86e942": {
                        "data": {
                            "settings": {
                                "locations": [],
                                "events": [
                                    {
                                        "name": "Monday Beginner Bachata Moves Class with Carlitos & Cece",
                                        "start": {"type": "datetime", "date": "2024-10-21", "time": "18:00"},
                                        "end": {"type": "datetime", "date": "2024-10-21", "time": "19:00"},
                                        "repeatPeriod": "weeklyOn",
                                        "repeatInterval": 1,
                                        "description": "<div>Phoenix Salsa Dance 2530 N 7th St Phoenix</div>",
                                        "buttonLink": {"value": "https://example.com/bachata"},
                                    },
                                    {
                                        "name": "Monday Intermediate Salsa Social Training",
                                        "start": {"type": "datetime", "date": "2024-05-27", "time": "19:00"},
                                        "end": {"type": "datetime", "date": "2024-05-27", "time": "21:00"},
                                        "repeatPeriod": "weeklyOn",
                                        "description": "<div>Join us at Phoenix Salsa Dance.</div>",
                                    },
                                    {
                                        "name": "Monday Beginner level 1 & 2 Salsa Partnering",
                                        "start": {"type": "datetime", "date": "2026-01-26", "time": "19:00"},
                                        "end": {"type": "datetime", "date": "2026-01-26", "time": "21:00"},
                                        "repeatPeriod": "weeklyOn",
                                        "description": "<div>Beginner friendly at Phoenix Salsa Dance.</div>",
                                    },
                                    {
                                        "name": "Hidden Team Training",
                                        "start": {"type": "datetime", "date": "2026-01-26", "time": "20:30"},
                                        "end": {"type": "datetime", "date": "2026-01-26", "time": "22:00"},
                                        "repeatPeriod": "weeklyOn",
                                        "visible": False,
                                        "description": "<div>Hidden</div>",
                                    },
                                    {
                                        "name": "Expired Wednesday Class",
                                        "start": {"type": "datetime", "date": "2026-01-07", "time": "19:00"},
                                        "end": {"type": "datetime", "date": "2026-01-07", "time": "20:30"},
                                        "repeatPeriod": "weeklyOn",
                                        "repeatEnds": "onDate",
                                        "repeatEndsDate": {"type": "datetime", "date": "2026-02-01", "time": "19:00"},
                                        "description": "<div>Expired</div>",
                                    },
                                ],
                            }
                        }
                    }
                }
            }
        }
        payloads = {
            PHOENIX_SALSA_DANCE_URL: calendar_html,
            "https://core.service.elfsight.com/p/boot/?w=ec2244a5-766f-4f93-883c-de4e5d86e942&page=https%3A%2F%2Fphoenixsalsadance.com%2Fcalendar%2F": json.dumps(boot_payload),
        }

        events = fetch_phoenix_salsa_dance(lambda url: payloads[url], date(2026, 3, 14))

        march_16_events = [event for event in events if event["start_at"].startswith("2026-03-16")]
        self.assertEqual(len(march_16_events), 3)
        self.assertEqual(
            {event["title"] for event in march_16_events},
            {
                "Monday Beginner Bachata Moves Class with Carlitos & Cece",
                "Monday Intermediate Salsa Social Training",
                "Monday Beginner level 1 & 2 Salsa Partnering",
            },
        )
        self.assertTrue(all(event["venue"] == "Phoenix Salsa Dance" for event in march_16_events))
        self.assertNotIn("Hidden Team Training", {event["title"] for event in events})
        self.assertNotIn("Expired Wednesday Class", {event["title"] for event in events})

    def test_phoenix_salsa_dance_parses_partial_meridiem_time_ranges(self) -> None:
        html = """
        <html>
          <body>
            <h2>Monday</h2>
            <p>Beginner Salsa Partnering</p>
            <p>Time: 7 - 8PM</p>
            <p>Price: $10</p>
            <p>Location:</p>
            <p>Phoenix Salsa Dance</p>
            <p>2530 N 7th St #107</p>
            <p>Phoenix, AZ 85006</p>
          </body>
        </html>
        """

        events = fetch_phoenix_salsa_dance(lambda url: html, date(2026, 3, 14))

        self.assertEqual(events[0]["title"], "Beginner Salsa Partnering")
        self.assertEqual(events[0]["start_at"], "2026-03-16T19:00:00-07:00")
        self.assertEqual(events[0]["end_at"], "2026-03-16T20:00:00-07:00")
        self.assertEqual(events[0]["venue"], "Phoenix Salsa Dance")
        self.assertEqual(events[0]["city"], "Phoenix")

    def test_phoenix_salsa_dance_accepts_class_titles_without_explicit_style_words(self) -> None:
        html = """
        <html>
          <body>
            <h2>Tuesday</h2>
            <p>Footwork Fusion Inter/ Advanced</p>
            <p>Time: 7 - 8:30PM</p>
            <p>Price: $15</p>
            <p>• Salsa footwork/shine</p>
            <p>Location:</p>
            <p>Phoenix Salsa Dance</p>
            <p>2530 N 7th St #107</p>
            <p>Phoenix, AZ 85006</p>
          </body>
        </html>
        """

        events = fetch_phoenix_salsa_dance(lambda url: html, date(2026, 3, 14))

        self.assertEqual(events[0]["title"], "Footwork Fusion Inter/ Advanced")
        self.assertEqual(events[0]["dance_style"], "Salsa")
        self.assertEqual(events[0]["venue"], "Phoenix Salsa Dance")

    def test_phoenix_salsa_dance_ignores_testimonial_copy_as_location(self) -> None:
        html = """
        <html>
          <body>
            <p>I’ve never danced salsa before and went there by myself, I absolutely love it!! The atmosphere in Phoenix is unmatched.</p>
            <h2>Sunday</h2>
            <p>Beginning Salsa Partnering 202</p>
            <p>Time: 2PM - 4PM Price: $20 • Drop in welcome</p>
          </body>
        </html>
        """

        events = fetch_phoenix_salsa_dance(lambda url: html, date(2026, 3, 14))

        self.assertEqual(events[0]["venue"], "")
        self.assertEqual(events[0]["city"], "Phoenix")
        self.assertEqual(events[0]["quality_note"], "Details may be incomplete; check the original source.")

    def test_phoenix_salsa_dance_drops_cta_fragments_from_notes(self) -> None:
        html = """
        <html>
          <body>
            <h2>Thursday</h2>
            <p>Advanced Salsa Partnering</p>
            <p>Time: 7 PM- 8:45 PM Price: $15 • Foundational combinations • Join Today! • Pay Here • Testimonials • Find us on Google!</p>
            <p>Copyright 2026</p>
          </body>
        </html>
        """

        events = fetch_phoenix_salsa_dance(lambda url: html, date(2026, 3, 14))

        self.assertEqual(events[0]["venue"], "")
        self.assertEqual(events[0]["notes"], "Price: $15 • Foundational combinations")

    def test_phoenix_salsa_dance_drops_footer_blob_from_notes(self) -> None:
        html = """
        <html>
          <body>
            <h2>Sunday</h2>
            <p>Beginning Salsa Partnering 202</p>
            <p>Time: 2PM - 4PM Price: $20 • Drop in welcome • Lead & Follow Technique</p>
            <p>Location: Phoenix Salsa Dance, 2530 N 7th St #107, Phoenix, AZ</p>
            <p>Contact Us • Text/Call: • 623.469.0123 • window.__NUXT__={reallyLongPayload:true}</p>
          </body>
        </html>
        """

        events = fetch_phoenix_salsa_dance(lambda url: html, date(2026, 3, 14))

        self.assertEqual(events[0]["city"], "Phoenix")
        self.assertEqual(events[0]["notes"], "Price: $20 • Drop in welcome • Lead & Follow Technique")

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

    def test_greater_phoenix_swing_ignores_contact_address_as_venue(self) -> None:
        html = """
        <html>
          <body>
            <p>P. O. Box 97459, Phoenix, Arizona 85060, United States 310.709.5709</p>
            <h2>Friday Night Dance</h2>
            <p>First Friday dance with lesson at 7:15 pm and social from 8:00 pm.</p>
            <p>Fatcat Ballroom, Mesa, AZ</p>
            <h2>First Sunday Swing Dance</h2>
            <p>Lesson starts at 5:30 pm and dancing begins at 6:15 pm.</p>
            <p>Fatcat Ballroom, Mesa, AZ</p>
          </body>
        </html>
        """

        events = fetch_greater_phoenix_swing(lambda url: html, date(2026, 3, 14))

        self.assertTrue(events)
        self.assertEqual(events[0]["venue"], "Fatcat Ballroom")
        self.assertEqual(events[0]["city"], "Mesa")

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

    def test_deduplicate_events_merges_fatcat_tuesday_alias_titles(self) -> None:
        events = [
            {
                "id": "one",
                "title": "East Coast Swing + Social Dancing – Fatcat",
                "start_at": "2026-03-17T19:00:00-07:00",
                "end_at": "2026-03-17T22:00:00-07:00",
                "all_day": False,
                "venue": "Fatcat Ballroom",
                "city": "Phoenix",
                "dance_style": "Swing",
                "source_name": "Swing Dancing Phoenix",
                "source_url": "https://one.example",
                "notes": "7 PM East Coast Swing lesson and social dancing.",
                "last_seen_at": "2026-03-14T00:00:00Z",
            },
            {
                "id": "two",
                "title": "Triple Step Tuesdays",
                "start_at": "2026-03-17T19:00:00-07:00",
                "end_at": "2026-03-17T21:30:00-07:00",
                "all_day": False,
                "venue": "Fatcat Ballroom",
                "city": "Phoenix",
                "dance_style": "Swing",
                "source_name": "Fatcat Ballroom",
                "source_url": "https://two.example",
                "notes": "7 PM East Coast Swing and Lindy Hop class followed by an 8 PM swing dance party.",
                "last_seen_at": "2026-03-14T00:00:00Z",
            },
        ]

        deduped = deduplicate_events(events)

        self.assertEqual(len(deduped), 1)
        self.assertIn("Fatcat Ballroom", deduped[0]["notes"])

    def test_fetch_nrg_ballroom_includes_only_partner_dances(self) -> None:
        month_html = """
        <html>
          <body>
            <a href="https://nrgballroom.com/event/sensual-bachata/2026-03-20/">Sensual Bachata</a>
            <a href="https://nrgballroom.com/event/monday-line-dancing/2026-03-23/">Monday Line Dancing</a>
            <a href="https://nrgballroom.com/event/sensual-bachata/2026-03-20/">Sensual Bachata</a>
            <a href="https://nrgballroom.com/events/month/2026-04/">Next Month</a>
          </body>
        </html>
        """
        partner_detail = """
        <html>
          <head>
            <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "Event",
                "name": "Sensual Bachata",
                "startDate": "2026-03-20T20:00:00-07:00",
                "endDate": "2026-03-20T23:00:00-07:00",
                "description": "Partner bachata night at NRG Ballroom.",
                "url": "https://nrgballroom.com/event/sensual-bachata/2026-03-20/",
                "location": {
                  "@type": "Place",
                  "name": "NRG Ballroom",
                  "address": {
                    "@type": "PostalAddress",
                    "streetAddress": "931 E Elliot Rd",
                    "addressLocality": "Tempe"
                  }
                }
              }
            </script>
          </head>
        </html>
        """
        non_partner_detail = """
        <html>
          <head>
            <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "Event",
                "name": "Monday Line Dancing",
                "startDate": "2026-03-23T19:00:00-07:00",
                "endDate": "2026-03-23T21:00:00-07:00",
                "description": "Beginner line dance class and open dancing.",
                "url": "https://nrgballroom.com/event/monday-line-dancing/2026-03-23/",
                "location": {
                  "@type": "Place",
                  "name": "NRG Ballroom",
                  "address": {
                    "@type": "PostalAddress",
                    "streetAddress": "931 E Elliot Rd",
                    "addressLocality": "Tempe"
                  }
                }
              }
            </script>
          </head>
        </html>
        """

        payloads = {
            NRG_BALLROOM_MONTH_URL: month_html,
            "https://nrgballroom.com/event/sensual-bachata/2026-03-20/": partner_detail,
            "https://nrgballroom.com/event/monday-line-dancing/2026-03-23/": non_partner_detail,
        }

        events = fetch_nrg_ballroom(lambda url: payloads[url], date(2026, 3, 15))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["title"], "Sensual Bachata")
        self.assertEqual(events[0]["venue"], "NRG Ballroom")
        self.assertEqual(events[0]["city"], "Tempe")

    def test_fetch_harolds_corral_includes_saturday_twostep_dances(self) -> None:
        payload = {
            "events": [
                {
                    "title": "SATURDAY BOOTS & DUKES DANCE LESSONS featuring the Silver Sage Band",
                    "description": "<p>Join in on the fun with our Dance Lessons hosted by Boots &amp; Dukes Dance Group at 7:30 pm and Live Music at 8:30 pm!</p>",
                    "start_date": "2026-03-14 19:30:00",
                    "end_date": "2026-03-15 01:00:00",
                    "url": "https://haroldscorral.com/event/saturday-boots-dukes/2026-03-14/",
                    "venue": {
                        "venue": "Harold&#8217;s Corral",
                        "city": "Cave Creek",
                    },
                },
                {
                    "title": "SATURDAY AFTERNOON with Last Train to Juarez",
                    "description": "<p>Enjoy Saturday afternoon with live music from 2 pm - 6 pm!</p>",
                    "start_date": "2026-03-14 14:00:00",
                    "end_date": "2026-03-14 18:00:00",
                    "url": "https://haroldscorral.com/event/saturday-afternoon-with-last-train-to-juarez/",
                    "venue": {
                        "venue": "Harold&#8217;s Corral",
                        "city": "Cave Creek",
                    },
                },
            ]
        }

        api_url = HAROLDS_CORRAL_API_URL + "?search=boots+dukes&per_page=100"
        events = fetch_harolds_corral(lambda url: json.dumps(payload) if url == api_url else "", date(2026, 3, 10))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["title"], "SATURDAY BOOTS & DUKES DANCE LESSONS featuring the Silver Sage Band")
        self.assertEqual(events[0]["dance_style"], "Two-Step")
        self.assertEqual(events[0]["venue"], "Harold’s Corral")
        self.assertEqual(events[0]["city"], "Cave Creek")

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

    def test_bachata_addiction_special_notes_ignore_script_noise(self) -> None:
        html = """
        <html>
          <body>
            <p>"Bachata N' Boots" Saturday March 14th!</p>
            <p>Lessons start at 8pm. Dancing 9:30-2am.</p>
            <script>window.__NUXT__={ giant: "payload" };</script>
          </body>
        </html>
        """

        events = fetch_bachata_addiction(lambda url: html, date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertNotIn("window.__NUXT__", events[0]["notes"])

    def test_shall_we_dance_decodes_escaped_ics_text(self) -> None:
        ics_text = """
BEGIN:VCALENDAR
BEGIN:VEVENT
DTSTART;TZID=America/Phoenix:20260320T190000
DTEND;TZID=America/Phoenix:20260320T220000
SUMMARY:Country Rain Country & Swing Dance
LOCATION:NRG Ballroom\\, 101\\, 931 E Elliot Rd\\, Tempe\\, AZ 85284\\, USA
DESCRIPTION:<u></u>Singles &amp\\; Couples welcome.\\nDance all night.
URL:https://shallwedancephoenix.com/country-rain
END:VEVENT
END:VCALENDAR
        """.strip()

        events = fetch_shall_we_dance(lambda url: ics_text, date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["city"], "Tempe")
        self.assertEqual(events[0]["venue"], "NRG Ballroom, 101, 931 E Elliot Rd")
        self.assertEqual(events[0]["source_url"], "https://shallwedancephoenix.com/country-rain")
        self.assertEqual(events[0]["notes"], "Singles & Couples welcome. Dance all night.")

    def test_meetup_filters_out_fitness_events(self) -> None:
        events = fetch_fatcat_meetup(lambda url: read_fixture("meetup_events.html"), date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["dance_style"], "West Coast Swing")

    def test_shall_we_dance_uses_public_ics_feed(self) -> None:
        events = fetch_shall_we_dance(lambda url: read_fixture("shall_we_dance.ics"), date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["city"], "Mesa")

    def test_rscds_source_uses_verified_class_location(self) -> None:
        events = fetch_rscds_phoenix(lambda url: read_fixture("rscds_classes.html"), date(2026, 3, 14))

        self.assertEqual(len(events), 8)
        self.assertTrue(all(event["venue"] == "Granite Reef Senior Center, 1700 N Granite Reef Rd" for event in events))
        self.assertTrue(all(event["city"] == "Scottsdale" for event in events))
        self.assertTrue(all("room 13/14" in event["notes"] for event in events))

    def test_shall_we_dance_skips_cancelled_ics_entries(self) -> None:
        ics_text = """
BEGIN:VCALENDAR
BEGIN:VEVENT
DTSTART;TZID=America/Phoenix:20260320T190000
DTEND;TZID=America/Phoenix:20260320T220000
SUMMARY:Country Rain Country & Swing Dance
LOCATION:NRG Ballroom\\, 101\\, 931 E Elliot Rd\\, Tempe\\, AZ 85284\\, USA
DESCRIPTION:Singles and couples welcome.
STATUS:CANCELLED
END:VEVENT
BEGIN:VEVENT
DTSTART;TZID=America/Phoenix:20260327T190000
DTEND;TZID=America/Phoenix:20260327T220000
SUMMARY:Friday Night Social
LOCATION:NRG Ballroom\\, 101\\, 931 E Elliot Rd\\, Tempe\\, AZ 85284\\, USA
DESCRIPTION:Still happening.
END:VEVENT
END:VCALENDAR
        """.strip()

        events = fetch_shall_we_dance(lambda url: ics_text, date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["title"], "Friday Night Social")

    def test_dancewise_recurring_sources_parse_core_schedule(self) -> None:
        events = fetch_dancewise(lambda url: read_fixture("dancewise_classes.html"), date(2026, 3, 14))

        self.assertGreaterEqual(len(events), 3)
        self.assertIn("Country", {event["dance_style"] for event in events})
        self.assertIn("Ballroom", {event["dance_style"] for event in events})

    def test_scootin_boots_builds_recurring_schedule(self) -> None:
        html = """
        <html>
          <body>
            <p>Group Classes | Mesa, AZ</p>
            <p>MONDAYS</p>
            <p>Morning Line Dancing</p>
            <p>Beginner: 9:00 - 9:45 AM</p>
            <p>Improver: 9:45 - 10:30 AM</p>
            <p>Intermediate: 10:30 - 11:30 AM</p>
            <p>Traditional Country Dancing Lessons With Mona Brandt</p>
            <p>Lessons With Mona Brandt: 6:00 - 9:00 PM</p>
            <p>Open Dancing</p>
            <p>9:00 -11:00 PM</p>
            <p>Country Swing</p>
            <p>Beginner and Intermediate Country Swing Lessons: 7:30 - 9:00 PM</p>
            <p>Open Dancing 9:00-11:00 PM</p>
            <p>THURSDAYS</p>
            <p>Traditional Country Dancing</p>
            <p>Beginner and Intermediate Partner Lessons: 6:30 - 8:00 PM</p>
            <p>Open Dancing: 8:00 - 10:00 PM</p>
            <p>West Coast Swing</p>
            <p>Beginner and Beyond the Basics Lessons: 6:30 - 8:00 PM</p>
            <p>Open Dancing: 8:00 - 10:00 PM</p>
            <p>FRIDAYS</p>
            <p>Country Swing</p>
            <p>Beginner and Intermediate Country Swing Lessons and 1 Line Dance: 8:00 - 9:00 PM</p>
            <p>Open Dancing: 9:00 PM - 12:00 AM</p>
            <p>SATURDAYS</p>
            <p>Morning Line Dancing</p>
            <p>Lessons 10:00 - 11:30 AM: Beginner and Improver Line Dances</p>
            <p>Open Dancing 11:30 - 1:00 PM</p>
          </body>
        </html>
        """

        events = fetch_scootin_boots(lambda url: html, date(2026, 3, 14))

        self.assertGreaterEqual(len(events), 6)
        self.assertIn("Mesa", {event["city"] for event in events})
        self.assertIn("Country Swing", {event["dance_style"] for event in events})
        self.assertIn("West Coast Swing", {event["dance_style"] for event in events})
        self.assertIn("2026-03-16T09:00:00-07:00", {event["start_at"] for event in events})
        self.assertIn(SCOOTIN_BOOTS_URL, {event["source_url"] for event in events})

    def test_bachata_addiction_builds_recurring_and_special_events(self) -> None:
        events = fetch_bachata_addiction(lambda url: read_fixture("bachata_addiction.html"), date(2026, 3, 14))

        self.assertTrue(any(event["activity_kind"] == "Special Event" for event in events))
        self.assertTrue(any(event["title"] == "Bachata Addiction Thursday Social" for event in events))

    def test_scottish_and_english_country_sources_expand_recurrences(self) -> None:
        today = date(2026, 3, 14)
        scottish_events = fetch_rscds_phoenix(lambda url: read_fixture("rscds_classes.html"), today)
        english_events = fetch_english_country(lambda url: read_fixture("english_country.html"), today)

        self.assertGreaterEqual(len(scottish_events), 4)
        self.assertGreaterEqual(len(english_events), 2)
        self.assertEqual(scottish_events[0]["dance_style"], "Scottish Country")

    def test_phoenix_4th_creates_special_event(self) -> None:
        html = """
        <html>
          <body>
            <p>July 2 - 5, 2026</p>
            <p>West Coast Swing convention in Scottsdale.</p>
          </body>
        </html>
        """

        events = fetch_phoenix_4th(lambda url: html, date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["activity_kind"], "Special Event")

    def test_desert_city_swing_builds_recurring_friday_social(self) -> None:
        html = """
        <html>
          <body>
            <h1>Weekly Dance</h1>
            <p>Friday Night Schedule</p>
            <p><strong>Beginner Lesson</strong></p>
            <p>7:00pm - 7:45pm</p>
            <p><strong>Intermediate Lesson</strong></p>
            <p>7:45pm - 8:30pm</p>
            <p><strong>Open Dance</strong></p>
            <p>8:30pm - 11:30pm</p>
            <p><strong>NRG Dance Studio</strong></p>
            <p>931 E Elliott Rd<br>Tempe, AZ 85284</p>
          </body>
        </html>
        """

        events = fetch_desert_city_swing(lambda url: html, date(2026, 3, 14))

        self.assertGreaterEqual(len(events), 4)
        self.assertEqual(events[0]["title"], "Desert City Swing Friday Dance")
        self.assertEqual(events[0]["dance_style"], "West Coast Swing")
        self.assertEqual(events[0]["activity_kind"], "Social")
        self.assertEqual(events[0]["venue"], "NRG Dance Studio")
        self.assertEqual(events[0]["city"], "Tempe")
        self.assertEqual(events[0]["start_at"], "2026-03-20T19:00:00-07:00")
        self.assertEqual(events[0]["end_at"], "2026-03-20T23:30:00-07:00")
        self.assertEqual(events[0]["source_url"], DESERT_CITY_SWING_URL)

    def test_azsalsa_tumbao_builds_recurring_friday_social(self) -> None:
        html = """
        <html>
          <body>
            <h1>TUMBAO Latin Fridays</h1>
            <p>$10 (pay at the door), 21 &amp; over, Fridays 9:15 PM - 2 AM</p>
            <p>9:15 PM - 10:30 PM: Three simultaneous nightclub-style social dance classes!</p>
            <p>10:30 PM - 2:00 AM: Social dancing with resident DJ BEN and guest DJs!</p>
            <p>TUMBAO | Latin Fridays is hosted @ EL PACIFICO Restaurant and Events Center, 1712 W Broadway Rd suite 108, Mesa, Arizona 85202</p>
          </body>
        </html>
        """

        events = fetch_azsalsa_tumbao(lambda url: html, date(2026, 3, 14))

        self.assertGreaterEqual(len(events), 4)
        self.assertEqual(events[0]["title"], "TUMBAO Latin Fridays")
        self.assertEqual(events[0]["dance_style"], "Salsa / Bachata")
        self.assertEqual(events[0]["activity_kind"], "Social")
        self.assertEqual(events[0]["venue"], "EL PACIFICO Restaurant and Events Center")
        self.assertEqual(events[0]["city"], "Mesa")
        self.assertEqual(events[0]["start_at"], "2026-03-20T21:15:00-07:00")
        self.assertEqual(events[0]["end_at"], "2026-03-21T02:00:00-07:00")
        self.assertEqual(events[0]["source_url"], AZSALSA_TUMBAO_URL)

    def test_swingdependance_builds_special_event_when_site_lists_future_year(self) -> None:
        html = """
        <html>
          <body>
            <h1>SWINGdepenDANCE 2026</h1>
            <p>July 3rd - July 6th</p>
            <p>West Coast Swing workshop weekend in Phoenix.</p>
          </body>
        </html>
        """

        events = fetch_swingdependance(lambda url: html, date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["title"], "SWINGdepenDANCE")
        self.assertEqual(events[0]["dance_style"], "West Coast Swing")
        self.assertEqual(events[0]["activity_kind"], "Special Event")
        self.assertEqual(events[0]["city"], "Phoenix")
        self.assertEqual(events[0]["source_url"], SWINGDEPENDANCE_URL)
        self.assertEqual(events[0]["start_at"], "2026-07-03T09:00:00-07:00")
        self.assertEqual(events[0]["end_at"], "2026-07-06T23:00:00-07:00")

    def test_latin_sol_builds_festival_and_preparty(self) -> None:
        html = """
        <html>
          <body>
            <h1>2026 INFO | Latin Sol Festival</h1>
            <p>April 10-11, Tempe Arizona</p>
            <p>Thursday April 9,2026</p>
            <p>Latin Sol Pre-Party @ The Duce</p>
            <p>Friday April 10, 2026</p>
            <p>Our first day of workshops including our signature Dance Matters lecture and our kick off social. Salsanama, a freestyle Salsa tournament, will also be happening on this day!</p>
          </body>
        </html>
        """

        events = fetch_latin_sol(lambda url: html, date(2026, 3, 14))

        self.assertEqual(len(events), 2)
        titles = {event["title"] for event in events}
        self.assertEqual(titles, {"Latin Sol Festival", "Latin Sol Pre-Party"})
        festival = next(event for event in events if event["title"] == "Latin Sol Festival")
        preparty = next(event for event in events if event["title"] == "Latin Sol Pre-Party")
        self.assertEqual(festival["city"], "Tempe")
        self.assertEqual(festival["dance_style"], "Salsa / Bachata")
        self.assertEqual(festival["start_at"], "2026-04-10T09:00:00-07:00")
        self.assertEqual(festival["end_at"], "2026-04-11T23:00:00-07:00")
        self.assertIn("The Duce", str(preparty["venue"]))
        self.assertEqual(preparty["city"], "Phoenix")
        self.assertEqual(preparty["start_at"], "2026-04-09T19:00:00-07:00")
        self.assertEqual(preparty["source_url"], LATIN_SOL_URL)

    def test_summer_swing_fest_builds_special_event(self) -> None:
        html = """
        <html>
          <body>
            <h1>Summer Swing Fest 202 6</h1>
            <p>Mesa, Arizona</p>
            <p>August 1 4 -1 6 , 202 6</p>
            <p>Featuring a variety of swing music!</p>
          </body>
        </html>
        """

        events = fetch_summer_swing_fest(lambda url: html, date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["title"], "Summer Swing Fest")
        self.assertEqual(events[0]["city"], "Mesa")
        self.assertEqual(events[0]["dance_style"], "Swing")
        self.assertEqual(events[0]["activity_kind"], "Special Event")
        self.assertEqual(events[0]["start_at"], "2026-08-14T09:00:00-07:00")
        self.assertEqual(events[0]["end_at"], "2026-08-16T23:00:00-07:00")
        self.assertEqual(events[0]["source_url"], SUMMER_SWING_FEST_URL)

    def test_white_rabbit_wcs_uses_current_structured_cards(self) -> None:
        html = """
        <html>
          <head>
            <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "ItemList",
                "itemListElement": [
                  {
                    "@type": "ListItem",
                    "position": 1,
                    "item": {
                      "@type": "DanceEvent",
                      "name": "Full Swing",
                      "description": "WEST COAST SWING SOCIAL DANCE",
                      "startDate": "2026-03-17",
                      "location": {
                        "@type": "Place",
                        "name": "The Imperial Ballroom",
                        "address": {
                          "@type": "PostalAddress",
                          "addressLocality": "Scottsdale"
                        }
                      }
                    }
                  },
                  {
                    "@type": "ListItem",
                    "position": 2,
                    "item": {
                      "@type": "DanceEvent",
                      "name": "Prescott BeatMob",
                      "description": "Northern Arizona social",
                      "startDate": "2026-03-25",
                      "location": {
                        "@type": "Place",
                        "name": "Hazeltine Theater",
                        "address": {
                          "@type": "PostalAddress",
                          "addressLocality": "Prescott"
                        }
                      }
                    }
                  }
                ]
              }
            </script>
          </head>
          <body>
            <section class="events-section">
              <div class="events-list">
                <div class="event-item" data-type="social">
                  <article class="event-card" data-event-card data-event-id="full-swing-1">
                    <span class="event-type"> social </span>
                    <div class="event-content">
                      <div class="event-header">
                        <h3 class="event-title">Full Swing</h3>
                      </div>
                      <div class="event-details">
                        <div class="detail-item">
                          <span class="detail-icon">🕰️</span>
                          <span>9:30 PM - 11:59 PM</span>
                        </div>
                        <div class="detail-item">
                          <span class="detail-icon">📍</span>
                          <button class="venue-link" data-venue-button data-address="Imperial Ballroom Dance Company, 15475 N Greenway Hayden Loop Suite 17 B, Scottsdale, AZ 85260, USA">
                            Imperial Ballroom
                          </button>
                        </div>
                      </div>
                      <div class="event-footer">
                        <span class="event-organizer">by Full Swing Team</span>
                      </div>
                    </div>
                  </article>
                  <dialog data-maps-dialog="full-swing-1"></dialog>
                  <dialog data-event-dialog="full-swing-1">
                    <div class="event-dialog-content">
                      <div class="dialog-description">
                        <p>Weekly west coast swing social.</p>
                      </div>
                    </div>
                  </dialog>
                </div>
                <div class="event-item" data-type="social">
                  <article class="event-card" data-event-card data-event-id="prescott-1">
                    <span class="event-type"> social </span>
                    <div class="event-content">
                      <div class="event-header">
                        <h3 class="event-title">Prescott BeatMob</h3>
                      </div>
                      <div class="event-details">
                        <div class="detail-item">
                          <span class="detail-icon">🕰️</span>
                          <span>7:00 PM - 10:00 PM</span>
                        </div>
                        <div class="detail-item">
                          <span class="detail-icon">📍</span>
                          <button class="venue-link" data-venue-button data-address="Hazeltine Theater, Prescott, AZ 86301, USA">
                            Hazeltine Theater
                          </button>
                        </div>
                      </div>
                      <div class="event-footer">
                        <span class="event-organizer">by Sky</span>
                      </div>
                    </div>
                  </article>
                  <dialog data-maps-dialog="prescott-1"></dialog>
                  <dialog data-event-dialog="prescott-1">
                    <div class="event-dialog-content">
                      <div class="dialog-description">
                        <p>Northern Arizona social.</p>
                      </div>
                    </div>
                  </dialog>
                </div>
              </div>
            </section>
            <section class="events-section past-events">
              <div class="events-list">
                <div class="event-item" data-type="social">
                  <article class="event-card" data-event-card data-event-id="past-1">
                    <span class="event-type"> social </span>
                    <div class="event-content">
                      <div class="event-header">
                        <h3 class="event-title">Past Event</h3>
                      </div>
                      <div class="event-details">
                        <div class="detail-item">
                          <span class="detail-icon">🕰️</span>
                          <span>7:00 PM - 9:00 PM</span>
                        </div>
                        <div class="detail-item">
                          <span class="detail-icon">📍</span>
                          <button class="venue-link" data-venue-button data-address="NRG Ballroom, Tempe, AZ 85284, USA">
                            NRG Ballroom
                          </button>
                        </div>
                      </div>
                      <div class="event-footer">
                        <span class="event-organizer">by Mike</span>
                      </div>
                    </div>
                  </article>
                  <dialog data-maps-dialog="past-1"></dialog>
                  <dialog data-event-dialog="past-1">
                    <div class="event-dialog-content">
                      <div class="dialog-description">
                        <p>Past event.</p>
                      </div>
                    </div>
                  </dialog>
                </div>
              </div>
            </section>
          </body>
        </html>
        """

        events = fetch_white_rabbit_wcs(lambda url: html, date(2026, 3, 14))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["title"], "Full Swing")
        self.assertEqual(events[0]["city"], "Scottsdale")
        self.assertEqual(events[0]["venue"], "Imperial Ballroom")
        self.assertEqual(events[0]["dance_style"], "West Coast Swing")
        self.assertEqual(events[0]["activity_kind"], "Social")
        self.assertEqual(events[0]["source_url"], WHITE_RABBIT_WCS_URL)
        self.assertEqual(events[0]["start_at"], "2026-03-17T21:30:00-07:00")

    def test_cdc_calendar_ocr_builds_month_events(self) -> None:
        html = """
        <html>
          <body>
            <img src="https://cdc.dance/wp-content/uploads/2026/02/March-2026-1.jpg" alt="March 2026">
          </body>
        </html>
        """
        observations = [
            {"x": 0.1642, "y": 0.3812, "width": 0.1250, "height": 0.0240, "text": "Progressive Tango"},
            {"x": 0.2078, "y": 0.4092, "width": 0.0349, "height": 0.0180, "text": "6 PM"},
            {"x": 0.4448, "y": 0.4231, "width": 0.1119, "height": 0.0240, "text": "West Coast Swing"},
            {"x": 0.4563, "y": 0.3984, "width": 0.0903, "height": 0.0256, "text": "7 PM Beginner"},
            {"x": 0.4462, "y": 0.3812, "width": 0.1105, "height": 0.0180, "text": "8 PM Intermediate"},
            {"x": 0.4650, "y": 0.3568, "width": 0.0728, "height": 0.0209, "text": "9 PM Social"},
            {"x": 0.7573, "y": 0.4231, "width": 0.0363, "height": 0.0200, "text": "6 PM"},
            {"x": 0.7151, "y": 0.4052, "width": 0.1206, "height": 0.0200, "text": "WCS Foundations"},
            {"x": 0.7369, "y": 0.3730, "width": 0.0771, "height": 0.0204, "text": "7 PM Salsa"},
            {"x": 0.9302, "y": 0.3932, "width": 0.0392, "height": 0.0182, "text": "12 PM"},
            {"x": 0.8677, "y": 0.3713, "width": 0.1017, "height": 0.0202, "text": "Introduction to"},
            {"x": 0.8576, "y": 0.3453, "width": 0.1163, "height": 0.0242, "text": "Hustle Workshop"},
        ]

        with patch("dance_calendar.sources._download_binary", return_value=b"image"), patch(
            "dance_calendar.sources._run_cdc_calendar_ocr",
            return_value=observations,
        ):
            events = fetch_cdc_calendar(lambda url: html, date(2026, 3, 14))

        titles = {event["title"] for event in events}
        self.assertIn("Progressive Tango", titles)
        self.assertIn("West Coast Swing Beginner", titles)
        self.assertIn("West Coast Swing Intermediate", titles)
        self.assertIn("West Coast Swing Social", titles)
        self.assertIn("WCS Foundations", titles)
        self.assertIn("Salsa", titles)
        self.assertIn("Introduction to Hustle Workshop", titles)

        hustle = next(event for event in events if event["title"] == "Introduction to Hustle Workshop")
        self.assertEqual(hustle["activity_kind"], "Special Event")
        self.assertEqual(hustle["dance_style"], "Hustle")
        self.assertEqual(hustle["start_at"], "2026-03-21T12:00:00-07:00")

        wcs_social = next(event for event in events if event["title"] == "West Coast Swing Social")
        self.assertEqual(wcs_social["activity_kind"], "Social")
        self.assertEqual(wcs_social["dance_style"], "West Coast Swing")
        self.assertEqual(wcs_social["source_url"], CDC_CALENDAR_URL)


class PipelineTests(TestCase):
    def test_exclude_non_social_dance_events_filters_fitness_classes(self) -> None:
        events = [
            make_event(
                title="Hot Hula Fitness",
                start_at="2026-03-17T18:30:00-07:00",
                end_at=None,
                venue="Creative Dance Collective",
                city="Mesa",
                dance_style="Social Dance",
                source_name="CDC Studios",
                source_url="https://cdc.dance/calendar/",
                notes="Follow-along fitness class.",
                quality_flags=["text_source"],
            ),
            make_event(
                title="West Coast Swing Beginner",
                start_at="2026-03-18T19:00:00-07:00",
                end_at="2026-03-18T20:00:00-07:00",
                venue="Creative Dance Collective",
                city="Mesa",
                dance_style="West Coast Swing",
                source_name="CDC Studios",
                source_url="https://cdc.dance/calendar/",
                notes="Partner dance lesson.",
                quality_flags=["text_source"],
            ),
        ]

        filtered = exclude_non_social_dance_events(events)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["title"], "West Coast Swing Beginner")

    def test_suppress_cdc_ocr_duplicates_prefers_white_rabbit_cdc_wcs(self) -> None:
        events = [
            make_event(
                title="Late Nite Vibe",
                start_at="2026-03-18T19:00:00-07:00",
                end_at="2026-03-18T23:30:00-07:00",
                venue="Creative Dance Collective",
                city="Mesa",
                dance_style="West Coast Swing",
                source_name="White Rabbit WCS",
                source_url="https://whiterabbitwcs.com/events/",
                notes="Structured source",
                quality_flags=["structured_source"],
            ),
            make_event(
                title="West Coast Swing Beginner",
                start_at="2026-03-18T19:00:00-07:00",
                end_at="2026-03-18T20:00:00-07:00",
                venue="Creative Dance Collective",
                city="Mesa",
                dance_style="West Coast Swing",
                source_name="CDC Studios",
                source_url="https://cdc.dance/calendar/",
                notes="OCR source",
                quality_flags=["text_source"],
            ),
            make_event(
                title="West Coast Swing Intermediate",
                start_at="2026-03-18T20:00:00-07:00",
                end_at="2026-03-18T21:00:00-07:00",
                venue="Creative Dance Collective",
                city="Mesa",
                dance_style="West Coast Swing",
                source_name="CDC Studios",
                source_url="https://cdc.dance/calendar/",
                notes="OCR source",
                quality_flags=["text_source"],
            ),
            make_event(
                title="WCS Foundations",
                start_at="2026-03-18T18:00:00-07:00",
                end_at="2026-03-18T19:00:00-07:00",
                venue="NRG Ballroom",
                city="Tempe",
                dance_style="West Coast Swing",
                source_name="CDC Studios",
                source_url="https://cdc.dance/calendar/",
                notes="Different venue",
                quality_flags=["text_source"],
            ),
        ]

        filtered = suppress_cdc_ocr_duplicates(events)

        self.assertEqual(len(filtered), 2)
        self.assertEqual({event["title"] for event in filtered}, {"Late Nite Vibe", "WCS Foundations"})

    def test_run_source_retries_and_reports_warning(self) -> None:
        source = SourceDefinition(
            "Retry Source",
            "https://retry.example",
            lambda fetch_text, today: [
                make_event(
                    title="Retry Social",
                    start_at="2026-03-20T19:00:00-07:00",
                    end_at="2026-03-20T22:00:00-07:00",
                    venue="Retry Ballroom",
                    city="Phoenix",
                    dance_style="Swing",
                    source_name="Retry Source",
                    source_url="https://retry.example/event",
                    notes=fetch_text("https://retry.example/feed"),
                    quality_flags=["structured_source"],
                )
            ],
        )
        calls = {"count": 0}

        def flaky_fetch(url: str) -> str:
            calls["count"] += 1
            if calls["count"] == 1:
                raise URLError("temporary")
            return "Recovered notes"

        with patch("dance_calendar.pipeline.fetch_text", side_effect=flaky_fetch), patch("dance_calendar.pipeline.sleep", return_value=None):
            source_run = run_source(source, today=date(2026, 3, 14))

        self.assertEqual(source_run["status"], "warning")
        self.assertEqual(source_run["counts"]["retries"], 1)
        self.assertEqual(source_run["counts"]["events"], 1)

    def test_build_event_catalog_with_report_handles_fail_open_sources(self) -> None:
        clean_source = SourceDefinition(
            "Clean Source",
            "https://clean.example",
            lambda fetch_text, today: [
                make_event(
                    title="Clean Social",
                    start_at="2026-03-20T19:00:00-07:00",
                    end_at="2026-03-20T22:00:00-07:00",
                    venue="Clean Ballroom",
                    city="Phoenix",
                    dance_style="Swing",
                    source_name="Clean Source",
                    source_url="https://clean.example/event",
                    notes="Clean event",
                    quality_flags=["structured_source"],
                )
            ],
        )
        suspicious_source = SourceDefinition(
            "Suspicious Source",
            "https://suspicious.example",
            lambda fetch_text, today: [
                make_event(
                    title="Suspicious Social",
                    start_at="2026-03-21T19:00:00-07:00",
                    end_at="2026-03-21T22:00:00-07:00",
                    venue="",
                    city="Phoenix",
                    dance_style="Salsa",
                    source_name="Suspicious Source",
                    source_url="https://suspicious.example/event",
                    notes="Location pending",
                    quality_flags=["text_source", "fallback_location"],
                )
            ],
        )
        failing_source = SourceDefinition(
            "Broken Source",
            "https://broken.example",
            lambda fetch_text, today: (_ for _ in ()).throw(RuntimeError("boom")),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manual_path = Path(temp_dir) / "manual.json"
            manual_path.write_text("[]")
            with patch("dance_calendar.pipeline.all_sources", return_value=[clean_source, suspicious_source, failing_source]):
                events, report = build_event_catalog_with_report(today=date(2026, 3, 14), manual_path=manual_path)

        self.assertEqual(len(events), 2)
        self.assertEqual(report["summary"]["sources_error"], 1)
        self.assertEqual(report["summary"]["sources_warning"], 1)
        self.assertEqual(report["summary"]["events_with_warnings"], 1)
        broken = next(source for source in report["sources"] if source["source_name"] == "Broken Source")
        self.assertEqual(broken["status"], "error")
        suspicious = next(source for source in report["sources"] if source["source_name"] == "Suspicious Source")
        self.assertEqual(suspicious["status"], "warning")

    def test_build_event_catalog_with_report_filters_cancelled_events(self) -> None:
        source = SourceDefinition(
            "Mixed Source",
            "https://mixed.example",
            lambda fetch_text, today: [
                make_event(
                    title="Friday Social",
                    start_at="2026-03-20T19:00:00-07:00",
                    end_at="2026-03-20T22:00:00-07:00",
                    venue="Dance Hall",
                    city="Phoenix",
                    dance_style="Swing",
                    source_name="Mixed Source",
                    source_url="https://mixed.example/friday",
                    notes="Live band and lesson.",
                    quality_flags=["structured_source"],
                ),
                make_event(
                    title="Saturday Social",
                    start_at="2026-03-21T19:00:00-07:00",
                    end_at="2026-03-21T22:00:00-07:00",
                    venue="Dance Hall",
                    city="Phoenix",
                    dance_style="Swing",
                    source_name="Mixed Source",
                    source_url="https://mixed.example/saturday",
                    notes="Cancelled due to venue issue.",
                    quality_flags=["structured_source"],
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manual_path = Path(temp_dir) / "manual.json"
            manual_path.write_text(
                json.dumps(
                    [
                        {
                            "title": "Manual Salsa Night",
                            "start_at": "2026-03-22T19:00:00-07:00",
                            "end_at": "2026-03-22T22:00:00-07:00",
                            "venue": "Manual Ballroom",
                            "city": "Phoenix",
                            "dance_style": "Salsa",
                            "notes": "CANCELLED for this week.",
                        }
                    ]
                )
            )
            with patch("dance_calendar.pipeline.all_sources", return_value=[source]):
                events, report = build_event_catalog_with_report(today=date(2026, 3, 14), manual_path=manual_path)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["title"], "Friday Social")
        self.assertEqual(report["summary"]["events_total"], 1)
        mixed_source = next(item for item in report["sources"] if item["source_name"] == "Mixed Source")
        self.assertEqual(mixed_source["counts"]["events"], 1)
