from next_gen_attribution.modeling.attribution import Attribution


class Markov_Attribution(Attribution):
    def __init__(
        self,
        workflow_mode: str = "dev",
        data_source: str = "local",
        model_version: str = "main_20221121",
    ):
        super().__init__(
            "markov",
            "tours",
            workflow_mode,
            data_source,
            model_version,
        )

    def train(self) -> None:
        pass
        # HB: (Matthew collaboration!)
