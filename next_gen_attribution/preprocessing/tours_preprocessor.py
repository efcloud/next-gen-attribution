from datetime import date
from typing import Tuple

import pandas as pd

from next_gen_attribution.preprocessing.preprocessor import Preprocessor

curr_date = date.today().strftime("%Y%m%d")


class ToursPreprocessor(Preprocessor):
    """implementation of a BU-specific preprocessor for Tours"""

    def __init__(
        self,
        workflow_mode: str = "dev",
        data_source: str = "local",
        spark_date: str = "20221116",
        data_tag: str = f"generated_{curr_date}",
    ) -> None:
        super().__init__("tours", workflow_mode, data_source, spark_date, data_tag)
        self._curr_date = curr_date

    def _pick_touchpoints(self, data: pd.DataFrame) -> None:
        self._non_touchpoints = ["_uid", "is_converted"]
        utm_source_columns = list(data.filter(regex="utm_source_"))
        utm_campaign_columns = list(data.filter(regex="utm_campaign_"))
        self._non_touchpoints.extend(utm_campaign_columns)
        self._touchpoints = utm_source_columns

    def _pick_columns(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        columns_to_keep = [
            "_uid",
            "ea_utm_source_0",
            "ea_utm_source_1",
            "ea_utm_source_2",
            "ea_utm_source_3",
            "ea_utm_source_4",
            "ea_utm_campaign_0",
            "ea_utm_campaign_1",
            "ea_utm_campaign_2",
            "ea_utm_campaign_3",
            "ea_utm_campaign_4",
            "et_utm_source_0",
            "et_utm_source_1",
            "et_utm_source_2",
            "et_utm_source_3",
            "et_utm_source_4",
            "et_utm_campaign_0",
            "et_utm_campaign_1",
            "et_utm_campaign_2",
            "et_utm_campaign_3",
            "et_utm_campaign_4",
            "utm_source_0",
            "utm_source_1",
            "utm_source_2",
            "utm_source_3",
            "utm_source_4",
            "utm_campaign_0",
            "utm_campaign_1",
            "utm_campaign_2",
            "utm_campaign_3",
            "utm_campaign_4",
        ]
        return data[columns_to_keep]

    def _unify_column_names(self, data: pd.DataFrame) -> pd.DataFrame:
        for substring in ["ea_", "et_", "_1", "_2", "_0", "_3", "_4"]:
            data.columns = data.columns.str.replace(substring, "")
        return data

    def _column_split(
        self, data: pd.DataFrame, non_categorical_columns: list
    ) -> Tuple[list, list]:
        categorical_columns = data.columns.tolist()
        for column in non_categorical_columns:
            categorical_columns.remove(column)
        return categorical_columns

    def _generate_touchpoint_indicator_columns(
        self, data: pd.DataFrame
    ) -> pd.DataFrame:
        categorical_columns = self._column_split(data, non_categorical_columns=["_uid"])
        data = pd.get_dummies(data, columns=categorical_columns)
        data = data.groupby(level=0, axis=1).sum()
        categorical_columns = self._column_split(data, non_categorical_columns=["_uid"])
        # HB (Alex, what did the hardcoded 15 here correspond to again? Was it the 3 different {ea,et,legacy} multiplied by the 5 touchpoints each?)
        data[categorical_columns] /= 15
        data[categorical_columns] = data[categorical_columns].astype(int)
        return data

    def _extract_converted_individual_ids(
        self, id_data: pd.DataFrame, conversion_data: pd.DataFrame
    ) -> pd.DataFrame:
        # build map between _uid and individual_id of the form (_uid, individual_id)
        id_data["individual_id"] = id_data["individual_id"].fillna(0).astype(int)
        # build map between conversions and individual_id of the form (individual_id, single_conversion_event)
        conversion_data = conversion_data.rename(
            columns={"Individual_id": "individual_id"}
        )
        conversion_data = conversion_data.groupby("individual_id", as_index=False).agg(
            {"SourceCode": "first"}
        )
        # build converted individuals of the form (_uid, is_converted) from an inner join
        converted_individuals = pd.merge(
            id_data, conversion_data, on="individual_id", how="inner"
        )
        converted_individuals["is_converted"] = 1
        return converted_individuals[["_uid", "is_converted"]]

    def _merge_in_converted_individual_ids(
        self, data: pd.DataFrame, converted_individuals: pd.DataFrame
    ) -> pd.DataFrame:
        data = pd.merge(data, converted_individuals, on="_uid", how="left")
        data["is_converted"] = data["is_converted"].fillna(0).astype(int)
        return data

    def _email_adobe_fix(self, data: pd.DataFrame) -> pd.DataFrame:
        # put all of adobe into email as per Vivek's 2022-11-17-1757 message "source=email and source=adobe is one and the same"
        data["utm_source_email"] = data["utm_source_email"] + data["utm_source_adobe"]
        data = data.drop(columns=["utm_source_adobe"])
        return data

    def _remove_touchpoint_repetition(self, data: pd.DataFrame) -> pd.DataFrame:
        # disregard repeated occurences of the same touchpoint by clipping the number of occurences to a max of 1
        for journey_column in self._touchpoints:
            data[journey_column] = data[journey_column].clip(0, 1)
        return data

    def _build_journey_vector(self, data: pd.DataFrame) -> pd.DataFrame:
        data["jvector"] = data.drop(columns=self._non_touchpoints).apply(tuple, axis=1)
        return data

    def etl(self) -> None:
        """
        Performs preprocessing and saves to local
        """
        self._logger.info(f"Getting lytics, id, and conversion data")
        lytics_data, id_data, conversion_data = self._get_data()

        self._logger.info(f"Picking columns from lytics data")
        data = self._pick_columns(lytics_data)

        self._logger.info(
            "Unifying column names across EA, ET, Legacy and 0-4 touchpoints"
        )
        data = self._unify_column_names(data)

        self._logger.info("Generating touchpoint indicator columns")
        data = self._generate_touchpoint_indicator_columns(data)

        self._logger.info(
            "Extracting converted individuals from an inner join of id and conversion data"
        )
        converted_individuals = self._extract_converted_individual_ids(
            id_data, conversion_data
        )

        self._logger.info("Merging in converted individuals to the data")
        data = self._merge_in_converted_individual_ids(data, converted_individuals)

        self._logger.info("Running Vivek's email-adobe fix")
        data = self._email_adobe_fix(data)

        self._logger.info(f"Picking touchpoints from data")
        self._pick_touchpoints(data)

        self._logger.info("Disregarding repeated occurences of the same touchpoint")
        data = self._remove_touchpoint_repetition(data)

        self._logger.info("Building user journey vectors")
        data = self._build_journey_vector(data)
        self._logger.info(
            f"There are {len(data['jvector'].value_counts())} unique user journeys in this dataset"
        )

        self._save_to_local(data)
