from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.acmad.api_retriever import APIRetriever
from hdx.scraper.acmad.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestAcmad",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                apiretriever = APIRetriever(configuration, retriever)
                available_data_types = apiretriever.get_available_data_types()
                assert len(available_data_types) == 12
                data_type = "cdi"
                dataset_info = available_data_types[data_type]
                assert dataset_info == {
                    "2015": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2016": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2017": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2018": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2019": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2020": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2021": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2022": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2023": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2024": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2025": {
                        "dekads": ["1", "11", "21"],
                        "months": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                        ],
                    },
                    "2026": {"dekads": ["1"], "months": ["01"]},
                    "latest_dekad": 1,
                    "latest_month": 1,
                    "latest_year": 2026,
                    "start_year": 2015,
                }
                zipped_tifs = apiretriever.process()
                pipeline = Pipeline(configuration, zipped_tifs)
                dataset = pipeline.generate_dataset(data_type, dataset_info)
                assert dataset == {
                    "caveats": "Each ZIP download contains the original scanned TIFF files for a "
                    "single calendar\n"
                    "year, including monthly and dekadal scans. The files are provided "
                    "in a ZIP archive\n"
                    "to preserve the original sources and may be extracted and "
                    "converted as needed.\n"
                    "\n"
                    "\n"
                    "The data may require interpretation with its building blocks such "
                    "as the vegetation,\n"
                    "soil moisture or rainfall to fully place the cause of the "
                    "drought, highly dependent\n"
                    "on the data source, in this case SPI, soil moisture anomaly and "
                    "vegetation anomaly\n"
                    "which may vary depending with number of weather stations sharing "
                    "data on a geographic location.",
                    "dataset_date": "[2015-01-01T00:00:00 TO 2026-01-31T23:59:59]",
                    "groups": [
                        {"name": "ago"},
                        {"name": "bdi"},
                        {"name": "ben"},
                        {"name": "bfa"},
                        {"name": "bwa"},
                        {"name": "caf"},
                        {"name": "civ"},
                        {"name": "cmr"},
                        {"name": "cod"},
                        {"name": "cog"},
                        {"name": "com"},
                        {"name": "cpv"},
                        {"name": "dji"},
                        {"name": "dza"},
                        {"name": "egy"},
                        {"name": "eri"},
                        {"name": "esh"},
                        {"name": "eth"},
                        {"name": "gab"},
                        {"name": "gha"},
                        {"name": "gin"},
                        {"name": "gmb"},
                        {"name": "gnb"},
                        {"name": "gnq"},
                        {"name": "ken"},
                        {"name": "lbr"},
                        {"name": "lby"},
                        {"name": "lso"},
                        {"name": "mar"},
                        {"name": "mdg"},
                        {"name": "mli"},
                        {"name": "moz"},
                        {"name": "mrt"},
                        {"name": "mus"},
                        {"name": "mwi"},
                        {"name": "nam"},
                        {"name": "ner"},
                        {"name": "nga"},
                        {"name": "rwa"},
                        {"name": "sdn"},
                        {"name": "sen"},
                        {"name": "sle"},
                        {"name": "som"},
                        {"name": "ssd"},
                        {"name": "stp"},
                        {"name": "swz"},
                        {"name": "syc"},
                        {"name": "tcd"},
                        {"name": "tgo"},
                        {"name": "tun"},
                        {"name": "tza"},
                        {"name": "uga"},
                        {"name": "zaf"},
                        {"name": "zmb"},
                        {"name": "zwe"},
                    ],
                    "methodology_other": "The Combined Drought Indicator (CDI) is derived from "
                    "the combination of SPI, SMA and\n"
                    "fAPAR, to identify areas with the potential to suffer "
                    "agricultural drought, areas\n"
                    "where the vegetation is already affected by drought "
                    "conditions, and areas in the\n"
                    "recovery process to normal conditions after a drought "
                    "episode.",
                    "name": "acmad-combined-drought-indicator",
                    "notes": "The Combined Drought Indicator (CDI) dataset developed at ACMAD is "
                    "designed as an\n"
                    "operational drought-monitoring product that synthesizes multiple "
                    "biophysical signals\n"
                    "into a single, coherent assessment of drought conditions across "
                    "Africa. Rather than\n"
                    "relying on a single indicator, the ACMAD CDI combines information "
                    "from rainfall-based\n"
                    "drought indices and satellite-derived vegetation condition metrics, "
                    "applying\n"
                    "rule-based logic to identify areas where these independent "
                    "indicators consistently\n"
                    "point to drought stress. This convergence approach reduces the risk "
                    "of false alarms\n"
                    "that can arise from short-term rainfall variability or vegetation "
                    "anomalies unrelated\n"
                    "to drought. The data exist in two forms one based on GPCC while "
                    "another based on Era5\n"
                    "precipitation.\n"
                    "\n"
                    "\n"
                    "The dataset is particularly useful in the African context, where "
                    "ground observations\n"
                    "are sparse and climatic gradients are strong across regions. By "
                    "using harmonized,\n"
                    "gridded datasets, the CDI provides a comparable drought signal "
                    "across national\n"
                    "boundaries, supporting regional coordination and continental-scale "
                    "analysis. Its\n"
                    "categorical outputs are intentionally simple and policy-ready, "
                    "making them suitable\n"
                    "for drought bulletins, early-warning communications, and high-level "
                    "decision support.\n"
                    "\n"
                    "\n"
                    "Importantly, the CDI is not intended to replace detailed drought "
                    "diagnostics or impact\n"
                    "assessments. Instead, it serves as a convergence and confirmation "
                    "tool that highlights\n"
                    "areas requiring closer analysis and expert interpretation. In this "
                    "role, the CDI acts\n"
                    "as a bridge between complex climate data and practical drought-risk "
                    "management,\n"
                    "enabling timely, consistent, and defensible drought monitoring at "
                    "regional and\n"
                    "continental scales.",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "climate hazards",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "climate-weather",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "drought",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "hazards and risk",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Africa - Monthly and dekadal combined drought indicator (CDI)",
                }
                assert dataset.get_resources() == [
                    {
                        "description": "CDI geotiffs by month and dekad for 2026",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2026",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2025",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2025",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2024",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2024",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2023",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2023",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2022",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2022",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2021",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2021",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2020",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2020",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2019",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2019",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2018",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2018",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2017",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2017",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2016",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2016",
                    },
                    {
                        "description": "CDI geotiffs by month and dekad for 2015",
                        "format": "geotiff",
                        "name": "cdi_geotiffs_2015",
                    },
                ]
