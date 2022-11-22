"""
Attribution object factory with member functions that are overridden by the following implementations:
1) shapley, for a attribution model based on Shapley values
2) markov, for a attribution model based on Markov chains using the removal effect
"""
__author__ = "HB"

import importlib

from next_gen_attribution.modeling.attribution import Attribution


class AttributionFactory:
    """object factory class for attribution models"""

    def __init__(
        self,
        model_type: str,
        business_unit: str,
        workflow_mode: str,
        data_source: str,
        model_version: str,
    ):
        self._model_type = model_type
        self._business_unit = business_unit
        self._workflow_mode = workflow_mode
        self._data_source = data_source
        self._model_version = model_version

    def _attribution_factory(self) -> Attribution:
        """
        non-public member function that instantiates the correct attribution model
        :returns: an object instantiated from one of {Ub_Tensorflow_Attribution, ...}
        """
        if self._model_type == "shapley":
            module = importlib.import_module(
                "next_gen_attribution.modeling.shapley_attribution"
            )
            implementation = getattr(module, "Shapley_Attribution")
            return implementation(
                self._workflow_mode,
                self._data_source,
                self._model_version,
            )
        elif self._model_type == "markov":
            module = importlib.import_module(
                "next_gen_attribution.modeling.markov_attribution"
            )
            implementation = getattr(module, "Markov_Attribution")
            return implementation(
                self._workflow_mode,
                self._data_source,
                self._model_version,
            )
        else:
            raise RuntimeError("model_type must be one of {'shapley', 'markov', ...}.")

    def train(self) -> None:
        """
        public member function that is overridden by model-specific scoring
        :returns: None (the model directly outputs plots)
        """
        return self._attribution_factory().train()
