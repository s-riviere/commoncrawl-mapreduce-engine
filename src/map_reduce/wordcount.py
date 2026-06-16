# wordcount.py
"""
Word-count MapReduce job.

mapper(data):
  data is a Common Crawl WET URL
  (e.g. "https://data.commoncrawl.org/crawl-data/.../file.warc.wet.gz").
  The worker downloads the file (caching it in /tmp/slr207-group1-bis/cache/),
  streams through WARC conversion records, and returns [(word, 1), ...].

reducer(key, values):
  Standard sum reducer.
"""

import gzip
import hashlib
import io
import os
import re
import urllib.request

CACHE_DIR = "/tmp/slr207-group1-bis/cache"
USER_AGENT = "SLR207-MapReduce/1.0"
# Only keep alphabetic tokens of at least 2 chars — strips URLs, numbers, noise.
_WORD_RE = re.compile(r"[a-z]{2,}")


def _cache_path(url: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    digest = hashlib.md5(url.encode()).hexdigest()[:12]
    return os.path.join(CACHE_DIR, f"wet_{digest}.gz")


def _download(url: str) -> str:
    """Download url to cache and return local path. No-op if already cached."""
    path = _cache_path(url)
    if not os.path.exists(path):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
        with open(path, "wb") as f:
            f.write(data)
    return path


def _iter_wet_text(gz_path: str):
    """Yield plain-text bodies from WARC conversion records in a WET .gz file."""
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="ignore") as f:
        in_conversion = False
        past_headers  = False
        for line in f:
            if line.startswith("WARC/1.0"):
                in_conversion = False
                past_headers  = False
            elif line.startswith("WARC-Type:"):
                in_conversion = line.strip().endswith("conversion")
            elif in_conversion and not past_headers and line.strip() == "":
                past_headers = True
            elif in_conversion and past_headers:
                yield line


def mapper(url: str) -> list[tuple[str, int]]:
    """Download a WET file from `url` and return word counts as (word, 1) pairs."""
    gz_path = _download(url)
    counts: dict[str, int] = {}
    for line in _iter_wet_text(gz_path):
        for word in _WORD_RE.findall(line.lower()):
            counts[word] = counts.get(word, 0) + 1
    # Return pre-aggregated counts — one (word, count) per unique word.
    # This drastically reduces payload size vs returning (word,1) for every token.
    return list(counts.items())


def reducer(key: str, values: list[int]) -> tuple[str, int]:
    """Sum all counts for a given word across all mappers."""
    return key, sum(values)
