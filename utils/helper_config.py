import os
import yaml

def load_yaml_config(
    file_path: str,
) -> dict:
    
    """
    Loads a specific YAML file from a folder and returns its contents as a dict.
    
    Args:
        folder_path (str): Path to the folder containing YAML files.
        file_name (str): The name of the YAML file to load.
    
    Returns:
        dict: Parsed YAML configuration.
    """

    config_path = os.path.join(file_path)

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"No such file: {file_path}")

    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)

    return config_data

def list_all_files_in_directory(
    directory: str
) -> list:
    
    """
    Lists all files in a directory

    Args:
        directory (str): Path to the directory

    Returns:
        List of file paths in the directory
    """

    all_files = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            all_files.append(os.path.join(root, file))

    return all_files