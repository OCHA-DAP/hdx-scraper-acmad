import asyncio
import logging
import time
from asyncio import to_thread
from pathlib import Path
from timeit import default_timer as timer

from hdx.api.configuration import Configuration
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class APIRetriever:
    """APIRetriever class"""

    def __init__(self, configuration: Configuration, retriever: Retrieve):
        self._configuration = configuration
        self._retriever = retriever
        self._base_url = configuration["base_url"]
        self._available_data_types = self.available_data_types()

    def available_data_types(self) -> dict:
        get_datasets_endpoint = self._configuration["get_datasets_endpoint"]
        url = f"{self._base_url}{get_datasets_endpoint}"
        return self._retriever.download_json(url)

    def get_available_data_types(self) -> dict:
        return self._available_data_types

    async def poll_status(
        self, data_type: str, url: str, timeout: int = 120, check_interval: int = 5
    ) -> tuple[bool, str]:
        start_time = time.time()

        while True:
            # Check timeout
            if time.time() - start_time > timeout:
                return False, "timed out"

            # Await the network call
            json = await to_thread(self._retriever.download_json, url)

            current_status = json.get("status")

            if current_status == "done":
                return True, json["download_url"]

            if current_status == "failed":
                return False, "failed"

            await asyncio.sleep(check_interval)

    async def _download_year(
        self, data_type: str, year: int
    ) -> tuple[str, int, Path | None]:
        """
        Helper method to handle the logic for a single year.
        This allows us to schedule all years concurrently.
        """
        try:
            download_endpoint = self._configuration["download_endpoint"].format(
                data_type, year, year
            )
            url = f"{self._base_url}{download_endpoint}"

            json = await to_thread(self._retriever.download_json, url)

            success, download_url = await self.poll_status(
                data_type, json["status_url"]
            )

            if success:
                # Only poll asynchronously. Download one file at a time.
                file_data = self._retriever.download_file(download_url)
                return data_type, year, file_data
            else:
                logger.error(
                    f"{data_type}: Download of data for {year} failed: {download_url}"
                )
                return data_type, year, None

        except Exception as e:
            logger.error(f"Exception downloading {year}: {e}")
            return data_type, year, None

    async def download(self) -> dict:
        semaphore = asyncio.Semaphore(5)

        # 2. Define a wrapper that respects the semaphore
        async def bounded_download(dt, y):
            async with semaphore:
                return await self._download_year(dt, y)

        tasks = []
        for data_type, data_type_info in self._available_data_types.items():
            dataset_info = self._configuration.get(data_type)
            if not dataset_info:
                continue
            start_year = data_type_info["start_year"]
            end_year = data_type_info["latest_year"]
            for year in range(start_year, end_year + 1):
                tasks.append(bounded_download(data_type, year))

        results = await asyncio.gather(*tasks)

        zipped_tifs = {}

        for data_type, year, path in results:
            if path is None:
                continue

            if data_type not in zipped_tifs:
                zipped_tifs[data_type] = {}

            zipped_tifs[data_type][year] = path
        return zipped_tifs

    def process(self) -> dict:
        """Runs rsync asynchronously to get output and error streams

        Returns:
            dict: zipped tif paths
        """

        start_time = timer()
        zipped_tifs = asyncio.run(self.download())
        logger.info(f"Execution time: {timer() - start_time} seconds")
        return zipped_tifs
