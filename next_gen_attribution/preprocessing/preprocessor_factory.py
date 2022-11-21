"""
Preprocessor object factory with member functions that are overridden by the following implementations:
1) ToursPreprocessor, for Tours
"""
__author__ = "HB"

import importlib

from next_gen_attribution.preprocessing.preprocessor import Preprocessor


class PreprocessorFactory:
    """object factory class for BU-specific preprocessors"""

    def __init__(
        self,
        business_unit: str,
        workflow_mode: str,
        data_source: str,
        spark_date: str,
        data_tag: str,
    ):
        self._business_unit = business_unit
        self._workflow_mode = workflow_mode
        self._data_source = data_source
        self._spark_date = spark_date
        self._data_tag = data_tag

    def _preprocessor_factory(self) -> Preprocessor:
        """
        non-public member function that instantiates the correct BU-specific preprocessor
        :returns: an object instantiated from one of {ToursPreprocessor, ...}
        """
        if self._business_unit == "tours":
            module = importlib.import_module(
                "next_gen_attribution.preprocessing.tours_preprocessor"
            )
            implementation = getattr(module, "ToursPreprocessor")
        else:
            raise RuntimeError("business_unit must be one of {'tours', ...}.")

        return implementation(
            self._workflow_mode,
            self._data_source,
            self._spark_date,
            self._data_tag,
        )

    def etl(self, **kwargs) -> None:
        """
        public member function that is overridden by BU-specific ETL
        :returns: None (the preprocessed data output are saved in csv format)
        """
        return self._preprocessor_factory().etl(**kwargs)
