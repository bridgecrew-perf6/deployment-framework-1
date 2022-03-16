from logging import Logger, StreamHandler, getLogger
import os
import sys

import yaml

from sqlalchemy.engine import create_engine

current_dirname = os.path.dirname(__file__)
# for the time being the script may be launched in two different environments (either inside docker container or
# in the outer world by airflow
if "LAUNCHED_AS" in os.environ and os.environ["LAUNCHED_AS"] == "DF":
    path_to_config = os.path.abspath(os.path.join(current_dirname, "../config/config.yaml"))
else:
    path_to_config = os.path.abspath(os.path.join(current_dirname, "../config/airflow-config.yaml"))

with open(path_to_config) as file:
    config = yaml.load(file)

connection_string = config["metadata_connection_string"]
metadata_engine = create_engine(connection_string)

test_metadata_connection_string = config["test_metadata_connection_string"]
test_metadata_engine = create_engine(test_metadata_connection_string)

model_drivers_location = config["model_drivers_location"]
sys.path.append(model_drivers_location)

drivers_location = config["drivers_location"]
sys.path.append(drivers_location)

graph_database_uri = config["graph_database_uri"]
graph_database_auth = (config["graph_database_auth"]["username"], config["graph_database_auth"]["password"])

dag_folder = config["dag_folder"]
airflow_owner = config["airflow_owner"]

processor_store = config["processor_store"]
path_to_dag_template = config["path_to_dag_template"]

path_to_models = config["path_to_models"]

path_to_sources = config["path_to_sources"]

sql_source_connection_string = config["sql_source_connection_string"]
postgres_pdm_connection_string = config["postgres_pdm_connection_string"]
postgres_pdm_localhost_connection_string = config["postgres_pdm_localhost_connection_string"]


def get_logger(config: dict,
               name: str = __name__) -> Logger:
    logger = getLogger(name)
    logger.setLevel(level=config["logging_level"])
    handler = StreamHandler(sys.stdout)
    handler.setLevel(level=config["handler_logging_level"])
    logger.addHandler(handler)

    return logger


logger = get_logger(config)

