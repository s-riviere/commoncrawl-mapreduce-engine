#!/usr/bin/env python3
"""
Download Common Crawl WET files (plain-text extracts) for MapReduce input.

Usage:
    python3 download_commoncrawl.py --output-dir DIR --num-files N [--crawl CRAWL_ID]

Examples:
  python3 download_commoncrawl.py --num-files 5
  python3 download_commoncrawl.py --output-dir /tmp/folder --num-files 10
  python3 download_commoncrawl.py --crawl CC-MAIN-2024-10 --num-files 3

After downloading, start your MapReduce cluster:
  python3 master.py
  python3 worker.py <MASTER_HOST>
"""

import argparse
import gzip
import os
import re
import urllib.request


BASE_URL = "https://data.commoncrawl.org/"
DEFAULT_CRAWL = "CC-MAIN-2024-10"
SPLIT_PATTERN = re.compile(r"^commoncrawl-(\d{4})\.txt$")


def get_wet_paths(crawl_id: str = DEFAULT_CRAWL) -> list[str]:
    """Fetch the list of WET file paths for a given crawl."""
    paths_url = f"{BASE_URL}crawl-data/{crawl_id}/wet.paths.gz"
    print(f"[DOWNLOAD] Fetching WET file list from {paths_url} ...")
    req = urllib.request.Request(paths_url, headers={"User-Agent": "SLR207-MapReduce/1.0"})
    
    with urllib.request.urlopen(req, timeout=30) as resp:
        with gzip.open(resp, "rt") as f:
            paths = [line.strip() for line in f if line.strip()]
            
    print(f"[DOWNLOAD] Found {len(paths)} WET files in {crawl_id}")
    return paths


def download_wet_file(wet_path: str, index: int, output_dir: str) -> str:
    """
    Download a single WET file and extract plain text records to a local file.
    Streaming directly from the network connection to keep memory usage at zero.
    """
    url = BASE_URL + wet_path
    output_path = os.path.join(output_dir, f"commoncrawl-{index:04d}.txt")
    print(f"[DOWNLOAD] Downloading split {index:04d}... ", end="")

    req = urllib.request.Request(url, headers={"User-Agent": "SLR207-MapReduce/1.0"})
    os.makedirs(output_dir, exist_ok=True)

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            # Stream network content directly to avoid holding full files in memory.
            with gzip.open(resp, "rt", encoding="utf-8", errors="ignore") as f:
                with open(output_path, "w", encoding="utf-8") as out:
                    for line in f:
                        if not line.startswith(("WARC/", "CONTENT-", "Content-", "Metadata-")):
                            out.write(line)
    except Exception:
        print("[FAILED]")
        raise

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"[OK] ({size_mb:.1f} MB)")
    return output_path


def get_existing_split_indices(output_dir: str) -> set[int]:
    """Return the set of split indices already present in output_dir."""
    existing_indices: set[int] = set()
    if not os.path.isdir(output_dir):
        return existing_indices

    for name in os.listdir(output_dir):
        match = SPLIT_PATTERN.match(name)
        if match:
            existing_indices.add(int(match.group(1)))

    return existing_indices


def main():
    parser = argparse.ArgumentParser(description="Download Common Crawl WET files for MapReduce.")
    parser.add_argument("-o", "--output-dir", required=True, help="Directory to store downloaded text files")
    parser.add_argument("-n", "--num-files", required=True, type=int, help="Number of WET files to download")
    parser.add_argument("-c", "--crawl", default=DEFAULT_CRAWL, help=f"Common Crawl ID (default: {DEFAULT_CRAWL})")
    parser.add_argument("--missing-only", action="store_true", help="Download only missing split files in the range [0, num-files)")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    wet_paths = get_wet_paths(args.crawl)
    n = min(args.num_files, len(wet_paths))
    print(f"[DOWNLOAD] Will download {n} WET files to {args.output_dir}")

    target_indices = list(range(n))
    if args.missing_only:
        existing_indices = get_existing_split_indices(args.output_dir)
        target_indices = [i for i in target_indices if i not in existing_indices]
        print(f"[DOWNLOAD] Missing-only mode: {len(target_indices)} missing split(s) out of {n} requested")

    downloaded = []
    for i in target_indices:
        path = download_wet_file(wet_paths[i], i, args.output_dir)
        downloaded.append(path)

    print(f"[DOWNLOAD] Done ! {len(downloaded)} files saved to {args.output_dir}")


if __name__ == "__main__":
    main()
