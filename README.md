# Phoenix Dance Calendar

Static Jekyll site plus Python ingestion scripts for aggregating Phoenix-area social dance events into `_data/events.json`.

## Local workflow

1. Generate or refresh event data:

   ```bash
   python3 scripts/build_events.py --report-output reports/source_health.json
   ```

2. Serve the Jekyll site:

   ```bash
   bundle install
   bundle exec jekyll serve
   ```

3. Run the scraper and dedupe tests:

   ```bash
   python3 -m unittest discover -s tests
   ```

## Sources

- Swing Dancing Phoenix via their public events API
- Desert City Swing weekly dance page
- AZSalsa / TUMBAO Latin Fridays page
- SWINGdepenDANCE annual event page
- Latin Sol Festival event page
- Summer Swing Fest event page
- Salsa Vida Phoenix calendar plus event detail pages
- Phoenix Argentine Tango embedded Google calendars
- Zouk Phoenix embedded Google calendar
- Phoenix Traditional Music and Dance Society contra dance page
- Greater Phoenix Swing Dance Club calendar page
- Phoenix Salsa Dance calendar page
- Dave & Buster's Tempe, filtered to dated dance events only
- Bachata Addiction / Phoenix Bachata
- DanceWise recurring classes and social nights
- Fatcat Ballroom recurring class pages
- NRG Ballroom monthly events calendar, filtered to partner-dance events only
- Harold's Cave Creek Corral Saturday Boots & Dukes dance nights, labeled as Two-Step
- Fatcat Ballroom Meetup group
- Shall We Dance Phoenix public calendar feed
- Phoenix 4th of July Dance Convention
- RSCDS Phoenix Branch classes
- Phoenix English Country Dancers
- Country Dance Community via `_data/manual_events.json`

## Data model

Generated events use this shape:

- `id`
- `title`
- `start_at`
- `end_at`
- `all_day`
- `venue`
- `city`
- `dance_style`
- `source_name`
- `source_url`
- `notes`
- `last_seen_at`
- `quality_flags`
- `quality_note`

The optional health report at `reports/source_health.json` captures per-source status, warnings, retries, and event summaries without being committed to the repo.

## Manual overrides

Add manual entries to `_data/manual_events.json` using the same schema. This is the intended path for image-driven or otherwise brittle sources such as `cdc.dance`.

## Venue alias dataset

Add vetted venue aliases, canonical names, and coordinates to `scripts/dance_calendar/known_venues.json`. The ingestion pipeline loads that dataset when remapping scraped organizer labels like `RSCDS Phoenix Branch` to a real venue/address.
