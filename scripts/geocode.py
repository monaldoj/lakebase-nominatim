#!/usr/bin/env python3
"""
Geocoding and Reverse Geocoding using Nominatim server.

This script provides both forward geocoding (address → coordinates)
and reverse geocoding (coordinates → address) functionality.

Requirements:
    pip install requests
"""

import os
import sys
import argparse
import json
from typing import Optional, Dict, List
from urllib.parse import urlencode
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests library not found")
    print("Install with: pip install requests")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    # Load environment variables from .env file
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    # python-dotenv not installed, skip loading .env
    pass


class NominatimGeocoder:
    """Client for Nominatim geocoding API."""

    def __init__(self, base_url: str = "http://localhost:8000", timeout: int = 30):
        """
        Initialize the geocoder.

        Args:
            base_url: Base URL of the Nominatim server
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout

    def search(
        self,
        query: str,
        limit: int = 5,
        country_codes: Optional[str] = None,
        view_box: Optional[str] = None,
        bounded: bool = False,
    ) -> List[Dict]:
        """
        Forward geocoding: Convert address/place name to coordinates.

        Args:
            query: Address or place name to search for
            limit: Maximum number of results to return (default: 5)
            country_codes: Comma-separated list of country codes to limit search (e.g., "us,ca")
            view_box: Preferred area to search in format "left,top,right,bottom" (e.g., "-122.5,37.8,-122.3,37.7")
            bounded: If True, restrict results to view_box area only

        Returns:
            List of result dictionaries with location information
        """
        params = {
            'q': query,
            'format': 'json',
            'limit': limit,
            'addressdetails': 1,
        }

        if country_codes:
            params['countrycodes'] = country_codes

        if view_box:
            params['viewbox'] = view_box
            if bounded:
                params['bounded'] = 1

        url = f"{self.base_url}/search?{urlencode(params)}"

        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request to Nominatim: {e}", file=sys.stderr)
            return []

    def reverse(
        self,
        lat: float,
        lon: float,
        zoom: int = 18,
    ) -> Optional[Dict]:
        """
        Reverse geocoding: Convert coordinates to address.

        Args:
            lat: Latitude
            lon: Longitude
            zoom: Level of detail (0-18, where 18 is building level, 10 is city level)

        Returns:
            Dictionary with address information, or None if not found
        """
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json',
            'zoom': zoom,
            'addressdetails': 1,
        }

        url = f"{self.base_url}/reverse?{urlencode(params)}"

        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request to Nominatim: {e}", file=sys.stderr)
            return None

    def lookup(self, osm_ids: List[str]) -> List[Dict]:
        """
        Lookup places by OSM ID.

        Args:
            osm_ids: List of OSM IDs (e.g., ["R123", "W456", "N789"])
                    R = Relation, W = Way, N = Node

        Returns:
            List of result dictionaries
        """
        params = {
            'osm_ids': ','.join(osm_ids),
            'format': 'json',
            'addressdetails': 1,
        }

        url = f"{self.base_url}/lookup?{urlencode(params)}"

        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request to Nominatim: {e}", file=sys.stderr)
            return []


def format_search_results(results: List[Dict]) -> str:
    """Format forward geocoding results for display."""
    if not results:
        return "No results found."

    output = []
    for i, result in enumerate(results, 1):
        output.append(f"\n{'='*60}")
        output.append(f"Result {i}:")
        output.append(f"{'='*60}")
        output.append(f"Display Name: {result.get('display_name', 'N/A')}")
        output.append(f"Coordinates:  {result.get('lat', 'N/A')}, {result.get('lon', 'N/A')}")
        output.append(f"Type:         {result.get('type', 'N/A')} ({result.get('class', 'N/A')})")
        output.append(f"Importance:   {result.get('importance', 'N/A')}")

        # Display address details if available
        if 'address' in result:
            output.append("\nAddress Details:")
            address = result['address']
            for key in ['house_number', 'road', 'city', 'county', 'state', 'postcode', 'country']:
                if key in address:
                    output.append(f"  {key.replace('_', ' ').title()}: {address[key]}")

    return '\n'.join(output)


def format_reverse_result(result: Optional[Dict]) -> str:
    """Format reverse geocoding result for display."""
    if not result:
        return "No result found for those coordinates."

    if 'error' in result:
        return f"Error: {result['error']}"

    output = []
    output.append(f"\n{'='*60}")
    output.append("Reverse Geocoding Result:")
    output.append(f"{'='*60}")
    output.append(f"Display Name: {result.get('display_name', 'N/A')}")
    output.append(f"Type:         {result.get('type', 'N/A')} ({result.get('class', 'N/A')})")

    # Display address details if available
    if 'address' in result:
        output.append("\nAddress Details:")
        address = result['address']
        for key in ['house_number', 'road', 'neighbourhood', 'suburb', 'city', 'county', 'state', 'postcode', 'country']:
            if key in address:
                output.append(f"  {key.replace('_', ' ').title()}: {address[key]}")

    return '\n'.join(output)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Geocoding and Reverse Geocoding using Nominatim",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Forward geocoding (address to coordinates)
  python scripts/geocode.py --search "1600 Amphitheatre Parkway, Mountain View, CA"
  python scripts/geocode.py --search "Eiffel Tower, Paris"

  # Reverse geocoding (coordinates to address)
  python scripts/geocode.py --reverse 37.4224764 -122.0842499
  python scripts/geocode.py --reverse 48.8584 2.2945

  # Limit results
  python scripts/geocode.py --search "Paris" --limit 3

  # Filter by country
  python scripts/geocode.py --search "Springfield" --country us

  # JSON output
  python scripts/geocode.py --search "London" --json

  # Custom server URL
  python scripts/geocode.py --search "Berlin" --url http://your-server:8080
"""
    )

    parser.add_argument(
        '--search',
        type=str,
        help='Address or place name to search for (forward geocoding)'
    )
    parser.add_argument(
        '--reverse',
        nargs=2,
        type=float,
        metavar=('LAT', 'LON'),
        help='Coordinates for reverse geocoding (latitude longitude)'
    )
    # Get default URL from environment or use localhost
    default_url = os.getenv('NOMINATIM_URL', 'http://localhost:8000')

    parser.add_argument(
        '--url',
        type=str,
        default=default_url,
        help=f'Nominatim server URL (default: {default_url})'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=5,
        help='Maximum number of results for forward geocoding (default: 5)'
    )
    parser.add_argument(
        '--country',
        type=str,
        help='Limit search to specific country code(s), comma-separated (e.g., us,ca)'
    )
    parser.add_argument(
        '--zoom',
        type=int,
        default=18,
        choices=range(0, 19),
        help='Zoom level for reverse geocoding, 0-18 (default: 18, building level)'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output raw JSON response'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=30,
        help='Request timeout in seconds (default: 30)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show debug information including configuration source'
    )

    args = parser.parse_args()

    # Show debug info if verbose
    if args.verbose:
        env_path = Path(__file__).parent.parent / ".env"
        print(f"Debug Info:")
        print(f"  .env file: {env_path} ({'exists' if env_path.exists() else 'not found'})")
        print(f"  NOMINATIM_URL from env: {os.getenv('NOMINATIM_URL', 'not set')}")
        print(f"  Using URL: {args.url}")
        print()

    # Validate arguments
    if not args.search and not args.reverse:
        parser.error("Must specify either --search or --reverse")

    if args.search and args.reverse:
        parser.error("Cannot specify both --search and --reverse")

    # Initialize geocoder
    geocoder = NominatimGeocoder(base_url=args.url, timeout=args.timeout)

    print(f"Using Nominatim server: {args.url}")
    print()

    # Perform geocoding
    if args.search:
        # Forward geocoding
        print(f"Searching for: {args.search}")
        if args.country:
            print(f"Country filter: {args.country}")
        print()

        results = geocoder.search(
            query=args.search,
            limit=args.limit,
            country_codes=args.country,
        )

        if args.json:
            print(json.dumps(results, indent=2))
        else:
            print(format_search_results(results))

    elif args.reverse:
        # Reverse geocoding
        lat, lon = args.reverse
        print(f"Reverse geocoding coordinates: {lat}, {lon}")
        print(f"Zoom level: {args.zoom}")
        print()

        result = geocoder.reverse(lat=lat, lon=lon, zoom=args.zoom)

        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(format_reverse_result(result))

    print()


if __name__ == "__main__":
    main()
