PYTHON ?= python3
BUNDLE ?= bundle

.PHONY: install update-events test build serve dev clean

install:
	$(BUNDLE) install

update-events:
	$(PYTHON) scripts/build_events.py

test:
	$(PYTHON) -m unittest discover -s tests

build:
	$(BUNDLE) exec jekyll build

serve:
	$(BUNDLE) exec jekyll serve

dev:
	$(MAKE) update-events
	$(MAKE) build
	$(MAKE) serve

clean:
	rm -rf _site .jekyll-cache .jekyll-metadata
