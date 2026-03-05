"""CLI entry point for post-hoc feature analysis.

Usage
-----
    python scripts/analyze_features.py output/
    python scripts/analyze_features.py output/ -m 0x1234 -o charts/
    python scripts/analyze_features.py output/ --start 2026-03-05T10:00 --end 2026-03-05T18:00
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from alpha_pipeline.analysis.report import generate_report
from alpha_pipeline.utils.logging import setup_logging, get_logger

logger = get_logger(__name__)


def _parse_datetime(s: str) -> datetime:
    """Parse ISO-ish datetime string."""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"Cannot parse datetime: {s!r}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate feature analysis charts from Parquet data.",
    )
    parser.add_argument(
        "input",
        type=Path,
        nargs="+",
        help="Parquet file(s) or directory containing Parquet files.",
    )
    parser.add_argument(
        "-m", "--market-id",
        type=str,
        default=None,
        help="Filter to a specific market ID.",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("charts"),
        help="Output directory for chart PNGs (default: charts/).",
    )
    parser.add_argument(
        "--start",
        type=_parse_datetime,
        default=None,
        help="Start time filter (ISO format, e.g. 2026-03-05T10:00).",
    )
    parser.add_argument(
        "--end",
        type=_parse_datetime,
        default=None,
        help="End time filter (ISO format, e.g. 2026-03-05T18:00).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (default: INFO).",
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    saved = generate_report(
        data_paths=args.input,
        output_dir=args.output,
        market_id=args.market_id,
        start=args.start,
        end=args.end,
    )

    if saved:
        logger.info("report_complete", charts=len(saved))
        for p in saved:
            print(f"  {p}")
    else:
        logger.warning("no_charts_generated")
        sys.exit(1)


if __name__ == "__main__":
    main()
