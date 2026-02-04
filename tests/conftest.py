from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def fixtures_dir():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def input_dir(fixtures_dir):
    return join(fixtures_dir, "input")


@pytest.fixture(scope="session")
def config_dir(fixtures_dir):
    return join("src", "hdx", "scraper", "acmad", "config")


@pytest.fixture(scope="session")
def configuration(config_dir):
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=join(config_dir, "project_configuration.yaml"),
    )
    configuration = Configuration.read()
    Country.countriesdata(False)
    locations = []
    for iso3 in configuration["countries"]:
        locations.append(
            {"name": iso3.lower(), "title": Country.get_country_name_from_iso3(iso3)}
        )
    Locations.set_validlocations(locations)
    Vocabulary._approved_vocabulary = {
        "tags": [
            {"name": tag}
            # Change tags below to match those needed in tests
            for tag in (
                "climate hazards",
                "climate-weather",
                "drought",
                "hazards and risk",
            )
        ],
        "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
        "name": "approved",
    }
    return configuration
