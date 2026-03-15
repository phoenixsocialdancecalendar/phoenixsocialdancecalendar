---
layout: default
title: About
permalink: /about/
---

## What this site does

Phoenix Dance Calendar combines multiple public calendars into one list of upcoming events across Phoenix, Tempe, Mesa, Scottsdale, Chandler, Glendale, and nearby cities.

## Current sources

- [Swing Dancing Phoenix](https://www.swingdancingphoenix.com/)
- [Salsa Vida Phoenix Calendar](https://www.salsavida.com/guides/arizona/phoenix/calendar/)
- [Phoenix Traditional Music and Dance Society](https://phxtmd.org/contra-dance)
- [Greater Phoenix Swing Dance Club](https://greaterphoenixswingdanceclub.com/calendar)
- [Phoenix Salsa Dance](https://phoenixsalsadance.com/calendar/)
- [Dave & Buster's Tempe](https://www.daveandbusters.com/us/en/about/locations/tempe) when the location page exposes actual dance event listings
- [Country Dance Community](https://cdc.dance/calendar/) via manual overrides when the source is image-only

## Maintenance

The generated events file lives at `_data/events.json`. Automated fetchers pull from structured or semi-structured sources, and `_data/manual_events.json` is available for corrections, missing events, or image-driven calendars.
