#!/usr/bin/env python3
"""
Download Common Crawl WET files (plain-text extracts) for MapReduce input.

Usage:
  python3 download_commoncrawl.py [--output-dir DIR] [--num-files N] [--crawl CRAWL_ID]

Examples:
  python3 download_commoncrawl.py --num-files 5
  python3 download_commoncrawl.py --output-dir /tmp/slr207-group1-bis/input --num-files 10
  python3 download_commoncrawl.py --crawl CC-MAIN-2024-10 --num-files 3

After downloading, use the output directory as input for map.py:
  python3 map.py main /tmp/slr207-group1-bis/input <n_workers>
"""

import argparse
import gzip
import io
import os
import urllib.request

BASE_URL = "https://data.commoncrawl.org/"
DEFAULT_OUTPUT_DIR = "/tmp/slr207-group1-bis/input"
DEFAULT_DATA_DIR = "/tmp/slr207-group1-bis/data"
DEFAULT_CRAWL = "CC-MAIN-2024-10"


def get_wet_paths(crawl_id: str = DEFAULT_CRAWL) -> list[str]:
    """Fetch the list of WET file paths for a given crawl."""
    paths_url = f"{BASE_URL}crawl-data/{crawl_id}/wet.paths.gz"
    print(f"[DOWNLOAD] Fetching WET file list from {paths_url} ...")
    req = urllib.request.Request(paths_url, headers={"User-Agent": "SLR207-MapReduce/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
    with gzip.open(io.BytesIO(raw), "rt") as f:
        paths = [line.strip() for line in f if line.strip()]
    print(f"[DOWNLOAD] Found {len(paths)} WET files in {crawl_id}")
    return paths


def download_wet_file(wet_path: str, index: int, output_dir: str = DEFAULT_DATA_DIR) -> str:
    """
    Download a single WET file and extract plain text records to a local file.
    Returns the path of the output file.
    """
    url = BASE_URL + wet_path
    output_path = os.path.join(output_dir, f"commoncrawl-{index:04d}.txt")
    print(f"[DOWNLOAD] ({index}) Downloading {url} ...")

    req = urllib.request.Request(url, headers={"User-Agent": "SLR207-MapReduce/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        compressed = resp.read()

    print(f"[DOWNLOAD] ({index}) Decompressing ({len(compressed) / 1024 / 1024:.1f} MB compressed) ...")
    with gzip.open(io.BytesIO(compressed), "rt", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    # Extract only the text content from WARC records (skip headers)
    text_parts = []
    in_content = False
    for line in content.split("\n"):
        if line.startswith("WARC/1.0"):
            in_content = False
        elif line.startswith("Content-Length:"):
            # Next blank line starts the content
            in_content = False
        elif in_content:
            text_parts.append(line)
        elif line == "" and not in_content:
            in_content = True

    text = "\n".join(text_parts)
    with open(output_path, "w", encoding="utf-8") as out:
        out.write(text)

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"[DOWNLOAD] ({index}) Saved {output_path} ({size_mb:.1f} MB)")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Download Common Crawl WET files for MapReduce.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                        help=f"Directory to store downloaded text files (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--num-files", type=int, default=3,
                        help="Number of WET files to download (default: 3)")
    parser.add_argument("--crawl", default=DEFAULT_CRAWL,
                        help=f"Common Crawl ID (default: {DEFAULT_CRAWL})")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Get list of available WET files
    wet_paths = get_wet_paths(args.crawl)

    n = min(args.num_files, len(wet_paths))
    print(f"[DOWNLOAD] Will download {n} WET files to {args.output_dir}")

    downloaded = []
    for i in range(n):
        path = download_wet_file(wet_paths[i], i, args.output_dir)
        downloaded.append(path)

    print(f"\n[DOWNLOAD] Done! {len(downloaded)} files saved to {args.output_dir}")
    print(f"[DOWNLOAD] Run MapReduce with:")
    print(f"  python3 map.py main {args.output_dir} <n_workers>")


if __name__ == "__main__":
    main()
