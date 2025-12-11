"""
Script to snapshot Google Sheets data as JSON files.
Run this before deployment or via cron to update static data.
"""
import asyncio
import json
from pathlib import Path
from datetime import datetime
from strength import _compute_strength
from schedule_strength import _compute_schedule_strength
from logger_config import setup_logging

logger = setup_logging("INFO")

# Data directory for snapshots
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

async def snapshot_all_data():
    """Fetch and save all endpoint data as JSON"""
    logger.info("=" * 80)
    logger.info("DATA SNAPSHOT: Starting data snapshot process...")
    logger.info("=" * 80)

    try:
        # Snapshot strength data
        logger.info("Fetching strength data...")
        strength_data = await _compute_strength()
        strength_file = DATA_DIR / "strength.json"
        with open(strength_file, 'w') as f:
            json.dump(strength_data, f, indent=2)
        logger.info(f"✓ Saved strength data to {strength_file}")

        # Snapshot schedule_strength data
        logger.info("Fetching schedule_strength data...")
        schedule_data = await _compute_schedule_strength()
        schedule_file = DATA_DIR / "schedule_strength.json"
        with open(schedule_file, 'w') as f:
            json.dump(schedule_data, f, indent=2)
        logger.info(f"✓ Saved schedule_strength data to {schedule_file}")

        # Save metadata
        metadata = {
            "snapshot_time": datetime.now().isoformat(),
            "files": {
                "strength": str(strength_file.name),
                "schedule_strength": str(schedule_file.name)
            }
        }
        metadata_file = DATA_DIR / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"✓ Saved metadata to {metadata_file}")

        logger.info("=" * 80)
        logger.info("DATA SNAPSHOT: Complete! All data saved successfully.")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"DATA SNAPSHOT FAILED: {str(e)}")
        logger.error("=" * 80)
        raise

if __name__ == "__main__":
    asyncio.run(snapshot_all_data())
