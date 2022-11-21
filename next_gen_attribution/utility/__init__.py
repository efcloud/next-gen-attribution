import os
import shutil
from datetime import datetime, timedelta

from next_gen_attribution.utility import logger

log = logger.init("utility_init_logger")

curr_dir = os.path.dirname(os.path.realpath(__file__))
_ROOT = os.path.join(curr_dir, "..", "..")
_MODULE_ROOT = os.path.join(curr_dir, "..")
_HADOOP_ROOT = "/home/hadoop/next-gen-attribution"

well_known_paths = {
    "ROOT": _ROOT,
    "WORKFLOW_ROOT": os.path.join(_ROOT, "workflows"),
    "HADOOP_WORKFLOW_ROOT": os.path.join(_HADOOP_ROOT, "workflows"),
    "CONFIG_DIR": os.path.join(_ROOT, "config"),
    "HADOOP_CONFIG_DIR": os.path.join(_HADOOP_ROOT, "config"),
    "DATA_ROOT_DIR": os.path.join(_ROOT, "data"),
    "TEST_DIR": os.path.join(_ROOT, "test"),
    "HADOOP_TEST_DIR": os.path.join(_HADOOP_ROOT, "test"),
    "DIST_DIR": os.path.join(_ROOT, "dist"),
    "HADOOP_DIST_DIR": os.path.join(_HADOOP_ROOT, "dist"),
    "SECRETS_DIR": os.path.join(_ROOT, "secrets"),
    "HADOOP_SECRETS_DIR": "/tmp/secrets/",
    "DATASETS_DIR": os.path.join(_ROOT, "datasets/"),
    "PREPROCESSED_DATA_DIR": os.path.join(_ROOT, "preprocessed_data/"),
    "MODEL_OUTPUT_DIR": os.path.join(_ROOT, "model_output/"),
    "PARAMS_DIR": os.path.join(_ROOT, "next_gen_attribution/modeling/params/"),
}


latest_creds = {
    "TOURS_SNOWFLAKE_KEY": "tours/snowflake_key",
    "MLFLOW": "shared/mlflow",
}

# similar to above but for directories
# dir_path set, env_var set/not set: use dir_path (create if missing)
# dir_path not set, env_var set: use env_var (create if missing)
# dir_path not set, env_var not set: return None
def check_dir(dir_path, env_var):
    if dir_path and os.path.isdir(dir_path):
        if os.path.isdir(dir_path):
            return dir_path
        else:
            create_dir(dir_path)
            return dir_path
    elif (not dir_path) and (env_var in os.environ):
        if os.path.isdir(os.environ[env_var]):
            return os.environ[env_var]
        else:
            create_dir(os.environ[env_var])
            return os.environ[env_var]
    else:
        log.warn("Could not find or create relevant directory. Returning `None`")


# some file and dir manipulation
def rm(file_path):
    log.info("Removing file at {}...".format(file_path))
    os.remove(file_path)


def rm_dir(dir_path):
    if os.path.exists(dir_path) and os.path.isdir(dir_path):
        log.info("Found dir at {}, removing...".format(dir_path))
        shutil.rmtree(dir_path)


def create_dir(dir_path, replace=True):
    if os.path.exists(dir_path) and os.path.isdir(dir_path):
        if replace == True:
            rm_dir(dir_path)
            log.info("Making dir at {}...".format(dir_path))
            os.makedirs(dir_path, exist_ok=True)
        else:
            log.info("Dir at {} already created!".format(dir_path))
    else:
        log.info("Did not find dir at {}, making...".format(dir_path))
        os.makedirs(dir_path, exist_ok=True)


# convert string to boolean
def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


# generate S3 urls
def generate_s3_workflow_scratch_url(workflow_type, workflow_name, workflow_step):
    return "workflows/{}/{}/{}/".format(workflow_type, workflow_name, workflow_step)


def absolute_s3_path_to_relative(absolute_s3_path):
    absolute_s3_path = clean_up_s3_key(absolute_s3_path)
    relative_paths = absolute_s3_path.split("s3://")[1].split("/")
    return {"bucket": relative_paths[0], "relative_path": "/".join(relative_paths[1:])}


def get_bucket_from_s3_path(absolute_s3_path):
    return absolute_s3_path.split("s3://")


def clean_up_s3_key(s3_key, is_dir=False):
    if is_dir and s3_key[-1] != "/":
        s3_key = s3_key + "/"
    elif not is_dir and s3_key[-1] == "/":
        s3_key = s3_key[0:-1]  # remove trailling slash
    s3_key = s3_key.replace("\\", "/")  # change forward slashes to backslash
    return s3_key.strip()  # strip white spaces


# filter out a list of files based on an available date range
def filter_data_files_with_date_range(data_files, date_range, date_regex=r"\d{8}"):
    if date_range == "all":
        return data_files
    else:
        # assume normal range
        date_range = date_range.split("-")
        if len(date_range) != 2:
            raise RuntimeError(
                'Date range must have exactly two dates separated by -, like "20200401-20200410"!'
            )
        start_date = datetime.strptime(date_range[0], "%Y%m%d")
        end_date = datetime.strptime(date_range[1], "%Y%m%d")
        days = (end_date - start_date).days
        date_list = [end_date - timedelta(days=x) for x in range(days + 1)]
        date_list = [x.strftime("%Y%m%d") for x in date_list]

        # filter the datafiles
        res = []
        for d in date_list:
            res += [x for x in data_files if d in x]
        return res


# parse a dict of arguments to a list
def parse_dict_args_to_list(d):
    # get a list of tuples of (key, val) pairs from arguments
    arg_keys = ["--{}".format(k) for k in d.keys()]
    arg_vals = [k for k in d.values()]
    arg_list = list(zip(arg_keys, arg_vals))
    # disregard bool flags set to false
    arg_list = [
        key_val_pair
        for key_val_pair in arg_list
        if str(key_val_pair[-1]).lower() != "false"
    ]
    arg_list = [str(i) for sl in arg_list for i in sl if str(i).lower() != "true"]
    return arg_list


# substitute variables within a script and return it as a string
# used for Google Ads secrets, for example (swapping out for new customer ID)
def variable_substitution(script_path, variables):
    if type(variables) != dict:
        raise RuntimeError("Please ensure variables list is a dict.")
    with open(script_path) as f:
        data = f.read().strip()
        for k in variables.keys():
            data = data.replace("${%s}" % k, variables[k])
    return data
