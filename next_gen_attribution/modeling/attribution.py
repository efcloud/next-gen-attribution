import os
from abc import ABC, abstractmethod
from typing import Tuple

import pandas as pd

from next_gen_attribution.utility import logger, well_known_paths
from next_gen_attribution.utility.utility import load_params

log = logger.init("Attribution Modeling on Dataset")


class Attribution(ABC):
    """abstract class for attribution models"""

    def __init__(
        self,
        model_type: str,
        business_unit: str,
        workflow_mode: str,
        data_source: str,
        model_version: str,
    ) -> None:
        self._model_type = model_type
        self._business_unit = business_unit
        self._workflow_mode = workflow_mode
        self._data_source = data_source
        self._model_version = model_version
        self._logger = logger.init(f"{business_unit}_preprocessor")

        self._params = load_params(
            os.path.join(
                well_known_paths["PARAMS_DIR"], self._model_type, "default.yaml"
            )
        )

        self._model_output_dir = well_known_paths["MODEL_OUTPUT_DIR"]
        if not os.path.exists(self._model_output_dir):
            os.makedirs(self._model_output_dir)

    def _get_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        preprocessed_data_fpath = os.path.join(
            well_known_paths["PREPROCESSED_DATA_DIR"],
            self._params["spark_date"],
            self._params["data_tag"],
            "preprocessed.csv",
        )
        return pd.read_csv(preprocessed_data_fpath)

    @abstractmethod
    def train(self, datasets_dict=None) -> None:
        """
        implements a specific attribution's model training
        :returns: None (currently, the model is not saved and instead directly outputs plots)
        """
