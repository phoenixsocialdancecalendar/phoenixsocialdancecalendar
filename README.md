# Phoenix Dance Calendar

Static Jekyll site plus Python ingestion scripts for aggregating Phoenix-area social dance events into `_data/events.json`.

## Local workflow

1. Generate or refresh event data:

   ```bash
   python3 scripts/build_events.py
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
- Salsa Vida Phoenix calendar plus event detail pages
- Phoenix Traditional Music and Dance Society contra dance page
- Greater Phoenix Swing Dance Club calendar page
- Phoenix Salsa Dance calendar page
- Dave & Buster's Tempe, filtered to dated dance events only
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

## Manual overrides

Add manual entries to `_data/manual_events.json` using the same schema. This is the intended path for image-driven or otherwise brittle sources such as `cdc.dance`.
