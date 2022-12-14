import itertools
import math
import os
from ast import literal_eval
from collections import defaultdict

import matplotlib.pyplot as plt
import pandas as pd

from next_gen_attribution.modeling.attribution import Attribution


class Shapley_Attribution(Attribution):
    def __init__(
        self,
        workflow_mode: str = "dev",
        data_source: str = "local",
        model_version: str = "main_20221121",
    ):
        super().__init__(
            "shapley",
            "tours",
            workflow_mode,
            data_source,
            model_version,
        )

    def _power_set(self, input_list):
        pset = [
            ",".join(list(j))
            for i in range(len(input_list))
            for j in itertools.combinations(input_list, i + 1)
        ]
        return pset

    def _pick_touchpoints(self, data: pd.DataFrame) -> None:
        # (TODO: this is duplicated from pick_touchpoints() in tours preprocessor, refactor)
        self._non_touchpoints = ["_uid", "is_converted"]
        utm_source_columns = list(data.filter(regex="utm_source_"))
        utm_campaign_columns = list(data.filter(regex="utm_campaign_"))
        self._non_touchpoints.extend(utm_campaign_columns)
        self._touchpoints = utm_source_columns

    def _journey_vector_conversions(self, data: pd.DataFrame) -> pd.DataFrame:
        jvector_conversion_df = data.groupby("jvector", as_index=False).agg(
            {"is_converted": "sum"}
        )
        jvector_conversion_df = jvector_conversion_df.rename(
            columns={"is_converted": "conversions"}
        )
        return jvector_conversion_df

    def _define_coalition(self, jvector_conversion_df: pd.DataFrame) -> pd.DataFrame:
        # ensure that the "jvector" column is of type tuple
        jvector_conversion_df["jvector"] = [
            literal_eval(x) for x in jvector_conversion_df["jvector"]
        ]
        coalition_conversion_df = pd.DataFrame()
        coalition_conversion_df["coalition"] = [
            tuple(map(lambda x, y: x * y, jvector, tuple(self._touchpoints)))
            for jvector in jvector_conversion_df["jvector"]
        ]
        coalition_conversion_df["coalition"] = coalition_conversion_df[
            "coalition"
        ].apply(lambda x: ",".join([element for element in x if element]))
        coalition_conversion_df["conversions"] = jvector_conversion_df["conversions"]
        return coalition_conversion_df

    def _construct_coalition_value_dictionary(
        self, coalition_conversion_df: pd.DataFrame
    ) -> dict:
        coalition_value_dict = dict(
            (coalition, conversions)
            for coalition, conversions in zip(
                coalition_conversion_df["coalition"],
                coalition_conversion_df["conversions"],
            )
        )
        return coalition_value_dict

    def _construct_subset_value_dictionary(
        self, coalition_value_dict: dict, cpset: list
    ) -> dict:
        subset_value_dict = dict()
        for subset in cpset:
            subset_value_dict[subset] = coalition_value_dict.get(subset, 0)
        return subset_value_dict

    def _shapley_values(self, subset_value_dict: dict) -> dict:
        n = len(self._touchpoints)
        shapley_values = defaultdict(int)
        for touchpoint in self._touchpoints:
            for coalition in subset_value_dict.keys():
                if touchpoint not in coalition.split(","):
                    coalition_cardinality = len(coalition.split(","))
                    coalition_with_touchpoint = coalition.split(",")
                    coalition_with_touchpoint.append(touchpoint)
                    coalition_with_touchpoint = ",".join(
                        sorted(coalition_with_touchpoint)
                    )
                    weight = (
                        math.factorial(coalition_cardinality)
                        * math.factorial(n - coalition_cardinality - 1)
                        / math.factorial(n)
                    )  # weight = |S|!(n-|S|-1)!/n!
                    contrib = (
                        subset_value_dict[coalition_with_touchpoint]
                        - subset_value_dict[coalition]
                    )  # marginal contribution = v(S U {i})-v(S)
                    shapley_values[touchpoint] += weight * contrib
            shapley_values[touchpoint] += (
                subset_value_dict[touchpoint] / n
            )  # add the term corresponding to the empty set
        return shapley_values

    def _plot_rescaled_shapley_values(self, shapley_values: dict) -> None:
        rescaled_shapley_values = {
            k: abs(v) / max(shapley_values.values()) for k, v in shapley_values.items()
        }
        fig, ax = plt.subplots(figsize=(3.5, 3))
        ax.bar(*zip(*rescaled_shapley_values.items()))
        ax.set_xticklabels(
            ["\n" * (i % 6) + l for i, l in enumerate(self._touchpoints)]
        )
        ax.tick_params(axis="both", which="major", labelsize=14)
        print(f"Saving plot of rescaled Shapley values...")
        fig_fpath = os.path.join(
            self._model_output_dir, self._model_version, "rescaled_shapley_values.png"
        )
        plt.savefig(fig_fpath)
        plt.show()
        plt.clf()
        plt.close()

    def train(self) -> None:
        self._logger.info(f"Getting preprocessed data")
        data = self._get_data()

        self._logger.info(f"Picking touchpoints from data")
        self._pick_touchpoints(data)

        self._logger.info(
            "Computing the observed sum of conversions generated by each journey vector"
        )
        jvector_conversion_df = self._journey_vector_conversions(data)
        self._logger.info(
            f"{len(jvector_conversion_df.loc[jvector_conversion_df['conversions']>0])} out of {len(jvector_conversion_df)} user journey types have generated some conversions"
        )

        self._logger.info(
            "Defining a coalition as the element-wise multiplication of a journey vector and the vector of journey column names"
        )
        coalition_conversion_df = self._define_coalition(jvector_conversion_df)

        self._logger.info(
            "Constructing coalition-value dictionary of the form {coalition: coalition_value}"
        )
        coalition_value_dict = self._construct_coalition_value_dictionary(
            coalition_conversion_df
        )

        self._logger.info("Computing the power set of the set of all touchpoints")
        cpset = self._power_set(self._touchpoints)

        self._logger.info(
            "Constructing subset-value dictionary of the form {subset: subset_value}"
        )
        subset_value_dict = self._construct_subset_value_dictionary(
            coalition_value_dict, cpset
        )
        self._logger.info(
            f"There are {sum(subset_value_dict.values())} out of 2^{len(self._touchpoints)} coalitions with non-zero value"
        )

        self._logger.info("Computing Shapley values")
        shapley_values = self._shapley_values(subset_value_dict)

        self._logger.info("Plotting rescaled Shapley values")
        self._plot_rescaled_shapley_values(shapley_values)
