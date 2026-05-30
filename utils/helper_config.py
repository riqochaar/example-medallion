import os
import yaml
from pyspark.sql.functions import col
from pyspark.sql.types import *

def load_yaml_config(file_name: str, folder_path: str = "../config/tables"):
    """
    Loads a specific YAML file from a folder and returns its contents as a dict.
    
    Args:
        folder_path (str): Path to the folder containing YAML files.
        file_name (str): The name of the YAML file to load.
    
    Returns:
        dict: Parsed YAML configuration.
    """

    config_path = os.path.join(folder_path, f"{file_name}.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"No such file: {config_path}")

    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)

    return config_data