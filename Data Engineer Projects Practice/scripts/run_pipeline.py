"""Run the local Raw-to-Gold pipeline and validate its outputs."""

import logging
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGES = (
    ("Raw to Silver Delta", "01_bronze_to_silver_delta.py"),
    ("Silver Delta to Gold", "02_silver_to_gold.py"),
    ("Pipeline validation", "03_validate_pipeline.py"),
)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("pipeline")

    for stage_name, script_name in STAGES:
        command = [sys.executable, str(PROJECT_ROOT / "scripts" / script_name)]
        logger.info("Starting %s", stage_name)
        try:
            subprocess.run(command, cwd=PROJECT_ROOT, check=True)
        except subprocess.CalledProcessError as error:
            logger.error(
                "%s failed with exit code %s",
                stage_name,
                error.returncode,
            )
            return error.returncode or 1
        logger.info("Completed %s", stage_name)

    logger.info("Full pipeline completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
