# HB: local invocation
# python preprocessing.py --businessUnit=tours --workflowMode=dev
"""
Create the dataset for use in attribution model training.
"""

import argparse
from datetime import date

from next_gen_attribution.preprocessing.preprocessor_factory import PreprocessorFactory
from next_gen_attribution.utility import logger

log = logger.init("preprocessing")
curr_date = date.today().strftime("%Y%m%d")

#############################################
# preprocessing
#############################################
def main(
    business_unit: str,
    workflow_mode: str,
    data_source: str,
    spark_date: str,
    data_tag: str,
) -> None:
    preprocessor = PreprocessorFactory(
        business_unit,
        workflow_mode,
        data_source,
        spark_date,
        data_tag,
    )
    preprocessor.etl()
    log.info(f"Successfully completed preprocessing data!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run preprocessing for attribution model"
    )
    parser.add_argument(
        "--businessUnit",
        action="store",
        default="tours",
        dest="business_unit",
        help="one of {tours, ...}",
    )
    parser.add_argument(
        "--workflowMode",
        action="store",
        default="dev",
        dest="workflow_mode",
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
        "--sparkDate",
        action="store",
        default="20221116",
        dest="spark_date",
        help="string that gives the spark generation date",
    )
    parser.add_argument(
        "--dataTag",
        action="store",
        default=f"generated_{curr_date}",
        dest="data_tag",
        help="string that tags the data, e.g., a date",
    )

    args = parser.parse_args()
    print(vars(args))
    for k, v in vars(args).items():
        exec(f"{k} = '{v}'")
    main(**vars(args))
