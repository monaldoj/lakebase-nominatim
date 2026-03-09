#!/usr/bin/env python3
"""
Download OpenStreetMap data files for Nominatim.

This script downloads OSM data files from Geofabrik or custom URLs.

Requirements:
    curl (command-line tool)
"""

import sys
import subprocess
import argparse
from pathlib import Path
from OSM_SOURCES import OSM_SOURCES

# # Popular OSM data sources from Geofabrik
# OSM_SOURCES = {
#     # Small test datasets
#     "monaco": "https://download.geofabrik.de/europe/monaco-latest.osm.pbf",
#     "liechtenstein": "https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf",

#     # Country examples
#     "germany": "https://download.geofabrik.de/europe/germany-latest.osm.pbf",
#     "france": "https://download.geofabrik.de/europe/france-latest.osm.pbf",
#     "italy": "https://download.geofabrik.de/europe/italy-latest.osm.pbf",
#     "spain": "https://download.geofabrik.de/europe/spain-latest.osm.pbf",
#     "uk": "https://download.geofabrik.de/europe/great-britain-latest.osm.pbf",
#     "usa": "https://download.geofabrik.de/north-america/us-latest.osm.pbf",
#     "canada": "https://download.geofabrik.de/north-america/canada-latest.osm.pbf",

#     # US States
#     "california": "https://download.geofabrik.de/north-america/us/california-latest.osm.pbf",
#     "virginia": "https://download.geofabrik.de/north-america/us/virginia-latest.osm.pbf",
#     "maryland": "https://download.geofabrik.de/north-america/us/maryland-latest.osm.pbf",
#     "new-york": "https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf",
#     "texas": "https://download.geofabrik.de/north-america/us/texas-latest.osm.pbf",
# }


def download_osm_file(url: str, output_dir: Path) -> Path:
    """Download OSM data file using curl."""
    filename = url.split("/")[-1]
    output_path = output_dir / filename

    if output_path.exists():
        print(f"File already exists: {output_path}")
        response = input("Use existing file? (y/n): ").lower()
        if response == 'y':
            print("✓ Using existing file")
            return output_path
        print("Deleting existing file...")
        output_path.unlink()

    print(f"Downloading from: {url}")
    print(f"Saving to: {output_path}")
    print()
    print("This may take a while depending on file size...")
    print()

    try:
        subprocess.run(
            ["curl", "-L", "-o", str(output_path), url],
            check=True,
        )
        print()
        print("✓ Download completed successfully")
        return output_path
    except subprocess.CalledProcessError as e:
        print(f"✗ Download failed: {e}")
        sys.exit(1)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download OpenStreetMap data for Nominatim",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download Monaco (small test dataset)
  python scripts/02_download_osm_data.py --region monaco

  # Download Germany
  python scripts/02_download_osm_data.py --region germany

  # Download from custom URL
  python scripts/02_download_osm_data.py --url https://download.geofabrik.de/europe/switzerland-latest.osm.pbf

  # Download multiple regions (import together in one build)
  python scripts/02_download_osm_data.py --region virginia --region maryland

Available regions:
  """ + "\n  ".join(sorted(OSM_SOURCES.keys()))
    )

    parser.add_argument(
        "--region",
        choices=list(OSM_SOURCES.keys()),
        action="append",
        dest="regions",
        metavar="REGION",
        help="Predefined region to download (can be specified multiple times)",
    )
    parser.add_argument(
        "--url",
        action="append",
        dest="urls",
        metavar="URL",
        help="Custom URL to download OSM data from (can be specified multiple times)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./osm_data"),
        help="Directory to store downloaded OSM files (default: ./osm_data)",
    )

    args = parser.parse_args()

    regions = args.regions or []
    urls = args.urls or []

    # Validate arguments
    if not regions and not urls:
        parser.error("Must specify at least one --region or --url")

    print("=" * 60)
    print("OSM Data Download")
    print("=" * 60)
    print()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {args.output_dir}")
    print()

    # Build list of (label, url) pairs to download
    downloads = []
    for region in regions:
        downloads.append((region, OSM_SOURCES[region]))
    for url in urls:
        downloads.append((url, url))

    # Download all files
    downloaded_files = []
    for i, (label, url) in enumerate(downloads, 1):
        print(f"--- File {i}/{len(downloads)}: {label} ---")
        osm_file = download_osm_file(url, args.output_dir)
        downloaded_files.append(osm_file)
        print()

    print("=" * 60)
    print("✓ All downloads completed!")
    print("=" * 60)
    print()
    for f in downloaded_files:
        print(f"  {f}")

    print()
    print("Next steps:")
    files_args = " ".join(str(f) for f in downloaded_files)
    print(f"  Run: bash scripts/03_build_nominatim_server.sh {files_args}")


if __name__ == "__main__":
    main()
