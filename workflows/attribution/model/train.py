# HB: local invocation
# python train.py --modelType=shapley --workflowMode=dev
"""
Train a attribution model on the training dataset.
"""
import argparse
from datetime import date

from next_gen_attribution.modeling.attribution_factory import AttributionFactory
from next_gen_attribution.utility import logger

log = logger.init("train")

curr_date = date.today().strftime("%Y%m%d")

#############################################
# training
#############################################
def main(
    model_type: str,
    business_unit: str,
    workflow_mode: str,
    data_source: str,
    model_version: str,
) -> None:
    log.info("Instantiating attribution object...")
    attribution = AttributionFactory(
        model_type=model_type,
        business_unit=business_unit,
        workflow_mode=workflow_mode,
        data_source=data_source,
        model_version=model_version,
    )
    attribution.train()
    log.info(
        f"Successfully trained a {model_type} attribution model for {business_unit}!"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train a attribution model")
    parser.add_argument(
        "--modelType",
        default="shapley",
        action="store",
        dest="model_type",
        choices=["shapley", "markov"],
        help="one of {shapley, markov,...}",
    )
    parser.add_argument(
        "--businessUnit",
        default="tours",
        action="store",
        dest="business_unit",
        help="business unit, one of {tours}",
    )
    parser.add_argument(
        "--workflowMode",
        default="dev",
        action="store",
        dest="workflow_mode",
        choices=["dev", "prod"],
        help="one of {dev, prod}",
    )
    parser.add_argument(
        "--dataSource",
        default="local",
        action="store",
        dest="data_source",
        choices=["local", "s3"],
        help="one of {local, s3}",
    )
    parser.add_argument(
        "--modelVersion",
        action="store",
        default=f"{curr_date}",
        dest="model_version",
        help="model version prepended to the path",
    )

    args = parser.parse_args()
    print(vars(args))

    for k, v in vars(args).items():
        exec(f"{k} = '{v}'")
    main(**vars(args))
