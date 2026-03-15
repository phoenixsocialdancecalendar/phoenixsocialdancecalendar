from __future__ import annotations

import argparse
from pathlib import Path

from dance_calendar.pipeline import build_event_catalog_with_report, write_events, write_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the Phoenix dance events data file.")
    parser.add_argument("--output", default="_data/events.json", help="Path to the generated events JSON file.")
    parser.add_argument(
        "--manual-input",
        default="_data/manual_events.json",
        help="Path to the manual events JSON file.",
    )
    parser.add_argument(
        "--report-output",
        default="",
        help="Optional path to write an internal source health report JSON file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    manual_path = Path(args.manual_input)
    events, report = build_event_catalog_with_report(manual_path=manual_path)
    write_events(events, output_path)
    if args.report_output:
        write_report(report, Path(args.report_output))
    print(f"Wrote {len(events)} events to {output_path}")


if __name__ == "__main__":
    main()
