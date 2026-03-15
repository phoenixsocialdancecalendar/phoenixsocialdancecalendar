from __future__ import annotations

import argparse
from pathlib import Path

from dance_calendar.pipeline import build_event_catalog, write_events


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the Phoenix dance events data file.")
    parser.add_argument("--output", default="_data/events.json", help="Path to the generated events JSON file.")
    parser.add_argument(
        "--manual-input",
        default="_data/manual_events.json",
        help="Path to the manual events JSON file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    manual_path = Path(args.manual_input)
    events = build_event_catalog(manual_path=manual_path)
    write_events(events, output_path)
    print(f"Wrote {len(events)} events to {output_path}")


if __name__ == "__main__":
    main()
