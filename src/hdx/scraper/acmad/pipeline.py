#!/usr/bin/python
"""ACMAD scraper"""

import logging
import time
from pathlib import Path

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._base_url = configuration["base_url"]

    def get_available_datasets(self):
        get_datasets_endpoint = self._configuration["get_datasets_endpoint"]
        url = f"{self._base_url}{get_datasets_endpoint}"
        return self._retriever.download_json(url)

    def poll_status(
        self, url, timeout: int = 60, check_interval: int = 2
    ) -> str | None:
        start_time = time.time()

        while True:
            # 1. Check if we have exceeded the timeout
            if time.time() - start_time > timeout:
                raise TimeoutError("Job timed out while waiting for status 'done'")

            # 2. Make the request
            json = self._retriever.download_json(url)

            # 3. Check the status key
            current_status = json.get("status")
            print(f"Current status: {current_status}")

            if current_status == "done":
                return json["download_url"]

            if current_status == "failed":
                return None

            # 4. Wait before polling again to be polite to the server
            time.sleep(check_interval)

    def bulk_download(
        self, dataset_name: str, dataset_info: dict
    ) -> tuple[int, int, dict]:
        start_year = dataset_info["start_year"]
        end_year = dataset_info["latest_year"]
        zipped_tifs = {}
        for year in range(start_year, end_year + 1):
            download_endpoint = self._configuration["download_endpoint"].format(
                dataset_name, year, year
            )
            url = f"{self._base_url}{download_endpoint}"
            json = self._retriever.download_json(url)
            download_url = self.poll_status(json["status_url"])
            if not download_url:
                logger.error(f"Download of dataset {dataset_name} failed!")
                return None
            zipped_tifs[year] = self._retriever.download_file(download_url)
        return start_year, end_year, zipped_tifs

    def generate_resource(self, zip_path: Path, year: int) -> Resource:
        resource = Resource(
            {
                "name": f"cdi_geotiffs_{year}",
                "description": f"CDI geotiffs by month and dekad for {year}",
            }
        )
        resource.set_format("zipped geotiff")
        resource.set_file_to_upload(zip_path)
        return resource

    def generate_dataset(self, data_type: str, dataset_info: dict) -> Dataset | None:
        start_year, end_year, zipped_tiffs = self.bulk_download(data_type, dataset_info)
        dataset = Dataset({"name": "acmad-combined-drought-indicator"})
        dataset.set_time_period_year_range(start_year, end_year)
        dataset.add_tags(
            ("climate hazards", "climate-weather", "drought", "hazards and risk")
        )
        # Only if needed
        dataset.set_subnational(True)
        dataset.add_country_locations(self._configuration["countries"])

        # Add resources here
        for year, zip_path in reversed(zipped_tiffs.items()):
            dataset.add_update_resource(self.generate_resource(zip_path, year))
        return dataset
