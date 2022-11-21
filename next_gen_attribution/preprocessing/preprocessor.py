import os
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
import yaml

from next_gen_attribution.utility import logger, well_known_paths

log = logger.init("Preprocessing on Dataset")


class Preprocessor(ABC):
    """abstract class for BU-specific preprocessors"""

    def __init__(
        self,
        business_unit: str,
        workflow_mode: str,
        data_source: str,
        spark_date: str,
        data_tag: str,
    ) -> None:
        self._business_unit = business_unit
        self._workflow_mode = workflow_mode
        self._data_source = data_source
        # date spark_feature_engineering was carried out
        self._spark_date = spark_date
        # data tag to store train/val/test/scoring processed splits under
        self._data_tag = data_tag
        self._logger = logger.init(f"{business_unit}_preprocessor")
        self._output_dir =  os.path.join(well_known_paths["PREPROCESSED_DATA_DIR"], f"{self._spark_date}", f"{self._data_tag}")
        self._output_fpath =  os.path.join(self._output_dir, "preprocessed.csv")
        if not os.path.exists(self._output_dir):
            os.makedirs(self._output_dir)

    def _get_data(self):
        lytics_data_fpath = os.path.join(
            well_known_paths["DATASETS_DIR"],
            f"{self._spark_date}/lytics_web_data_last_5_expanded.csv",
        )
        id_data_fpath = os.path.join(
            well_known_paths["DATASETS_DIR"],
            f"{self._spark_date}/lytics_cleaned.csv",
        )
        conversion_data_fpath = os.path.join(
            well_known_paths["DATASETS_DIR"],
            f"{self._spark_date}/conversion.csv",
        )
        lytics_data = pd.read_csv(lytics_data_fpath, low_memory=False)
        id_data = pd.read_csv(id_data_fpath, low_memory=False)
        conversion_data = pd.read_csv(conversion_data_fpath, low_memory=False)
        return lytics_data, id_data, conversion_data

    def _save_to_local(
        self, preprocessed_data: pd.DataFrame
    ):
        """
        saves preprocessed data to local
        """
        log.info(
            f"Saving preprocessed data to {self._output_fpath}..."
        )
        preprocessed_data.to_csv(self._output_fpath, index=False)

    @abstractmethod
    def etl(self) -> None:
        """
        implements BU-specific ETL to generate preprocessed data used in subsequent modeling
        :returns: None (the preprocessed data output are saved in csv format)
        """

