"""
Utility class for dumping Spark DataFrames to CSV files for debugging purposes.
"""

import os
import logging
from pyspark.sql import DataFrame

from utils.location_pipeline_config_manager import LocationPipelineConfig

logger = logging.getLogger(__name__)


class DataFrameDumper:
    """
    Handles dumping Spark DataFrames to CSV files.

    Collects data to the driver and writes locally, ensuring files are written
    to the machine running the driver (not distributed across executors).

    Output directory is configurable via PIPELINE_OUTPUT_DIR environment variable.
    Defaults to /tmp/spark-pipeline-output.
    """

    def __init__(self, location_pipeline_config: LocationPipelineConfig):
        """
        Initialize the dumper with an output directory.

        Args:
            output_dir: Directory path for CSV output. If None, uses
                       PIPELINE_OUTPUT_DIR env var or defaults to /tmp/spark-pipeline-output
        """
        self.output_dir = location_pipeline_config.output_data_dir
        self._ensure_output_dir()
        logger.info(f"DataFrameDumper initialized with output directory: {self.output_dir}")

    def _ensure_output_dir(self):
        """Create output directory if it doesn't exist."""
        os.makedirs(self.output_dir, exist_ok=True)

    def dump(self, df: DataFrame, name: str) -> str:
        """
        Dump a DataFrame to CSV by collecting to driver.

        This approach ensures the CSV is written on the driver's filesystem,
        which is necessary when driver and executors don't share a filesystem.

        Args:
            df: Spark DataFrame to dump
            name: Name for the output file (will create {name}.csv)

        Returns:
            Path to the output file
        """
        output_path = os.path.join(self.output_dir, f"{name}.csv")

        logger.info(f"Collecting DataFrame to driver and dumping to {output_path}")

        # Collect to driver and convert to pandas for local file write
        pdf = df.toPandas()
        pdf.to_csv(output_path, index=False)

        logger.info(f"Successfully dumped {len(pdf)} rows to {output_path}")
        return output_path

    def dump_with_partition(self, df: DataFrame, name: str, partition_by: str) -> str:
        """
        Dump a DataFrame to CSV, with one file per partition value.

        Args:
            df: Spark DataFrame to dump
            name: Base name for output files
            partition_by: Column name to partition by

        Returns:
            Path to the output directory
        """
        output_subdir = os.path.join(self.output_dir, name)
        os.makedirs(output_subdir, exist_ok=True)

        logger.info(f"Dumping DataFrame to {output_subdir} (partitioned by {partition_by})")

        # Collect to driver
        pdf = df.toPandas()

        # Write one file per partition value
        for value, group in pdf.groupby(partition_by):
            file_path = os.path.join(output_subdir, f"{partition_by}={value}.csv")
            group.to_csv(file_path, index=False)

        logger.info(f"Successfully dumped {len(pdf)} rows to {output_subdir}")
        return output_subdir
