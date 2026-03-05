"""Report orchestrator — generates all charts for loaded feature data."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from alpha_pipeline.analysis.charts import CHART_REGISTRY
from alpha_pipeline.analysis.loader import load_feature_vectors, vectors_to_feature_frames
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


def generate_report(
    data_paths: list[Path],
    output_dir: Path,
    *,
    market_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[Path]:
    """Load feature data and generate all available charts.

    Parameters
    ----------
    data_paths:
        Parquet files or directories containing them.
    output_dir:
        Where to save the PNG charts.
    market_id / start / end:
        Optional filters passed to ``load_feature_vectors``.

    Returns
    -------
    List of saved chart file paths.
    """
    vectors = load_feature_vectors(
        data_paths, market_id=market_id, start=start, end=end,
    )
    if not vectors:
        logger.warning("no_data", msg="No feature vectors found for the given filters")
        return []

    logger.info("vectors_loaded", count=len(vectors))
    frames = vectors_to_feature_frames(vectors)
    logger.info("features_found", features=list(frames.keys()))

    saved: list[Path] = []
    for feature_name, plot_fn in CHART_REGISTRY.items():
        if feature_name not in frames:
            logger.info("chart_skipped", feature=feature_name, reason="no data")
            continue

        df = frames[feature_name]
        if df.is_empty():
            logger.info("chart_skipped", feature=feature_name, reason="empty dataframe")
            continue

        try:
            result = plot_fn(df, output_dir)
            if isinstance(result, list):
                saved.extend(result)
                for p in result:
                    logger.info("chart_saved", feature=feature_name, path=str(p))
            else:
                saved.append(result)
                logger.info("chart_saved", feature=feature_name, path=str(result))
        except Exception:
            logger.exception("chart_error", feature=feature_name)

    return saved
