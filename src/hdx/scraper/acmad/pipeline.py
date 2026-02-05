#!/usr/bin/python
"""ACMAD scraper"""

import logging
from calendar import monthrange
from pathlib import Path

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dateparse import parse_date

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, zipped_tifs: dict):
        self._configuration = configuration
        self._zipped_tifs = zipped_tifs

    @staticmethod
    def generate_resource(resource_info: dict, zip_path: Path, year: int) -> Resource:
        resource = Resource(
            {
                "name": resource_info["name"].format(year),
                "description": resource_info["description"].format(year),
            }
        )
        resource.set_format("zipped geotiff")
        resource.set_file_to_upload(zip_path)
        return resource

    def generate_dataset(self, data_type: str, data_type_info: dict) -> Dataset | None:
        dataset_info = self._configuration.get(data_type)
        if not dataset_info:
            return None
        logger.info(f"{data_type}: Generating dataset...")
        start_year = data_type_info["start_year"]
        end_year = data_type_info["latest_year"]
        end_month = data_type_info["latest_month"]
        zipped_tifs = self._zipped_tifs[data_type]
        if len(zipped_tifs) == 0:
            logger.error(f"{data_type}: No data available!")
            return None
        actual_start_year = min(zipped_tifs)
        if actual_start_year != start_year:
            logger.warning(
                f"{data_type}: Start year {start_year} not correct. Changing to {actual_start_year}"
            )
            start_year = actual_start_year
        actual_end_year = max(zipped_tifs)
        if actual_end_year != end_year:
            logger.warning(
                f"{data_type}: End year {end_year} not correct. Changing to {actual_end_year}"
            )
            end_year = actual_end_year
            end_month = 12
        dataset_name = dataset_info["name"]
        dataset_title = dataset_info["title"]
        dataset_notes = dataset_info["notes"]
        dataset_methodology = dataset_info["methodology"]
        dataset_caveats = dataset_info["caveats"]
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
                "notes": dataset_notes,
                "methodology_other": dataset_methodology,
                "caveats": dataset_caveats,
            }
        )
        start_date = parse_date(f"{start_year}-01-01")
        _, max_days = monthrange(end_year, end_month)
        end_date = parse_date(f"{end_year}-{end_month}-{max_days}")
        dataset.set_time_period(start_date, end_date)
        dataset.add_tags(
            ("climate hazards", "climate-weather", "drought", "hazards and risk")
        )
        # Only if needed
        dataset.set_subnational(True)
        dataset.add_country_locations(self._configuration["countries"])

        # Add resources here
        resource_info = dataset_info["resource"]
        for year, zip_path in reversed(zipped_tifs.items()):
            dataset.add_update_resource(
                self.generate_resource(resource_info, zip_path, year)
            )
        return dataset
