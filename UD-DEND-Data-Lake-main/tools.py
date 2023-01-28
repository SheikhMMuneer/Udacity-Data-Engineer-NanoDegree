import configparser
import os
from datetime import datetime

def read_config(config_path):
    """
    Description: Reads config from config file. Needs to be executed first

    Arguments:
        config_path: path to config file

    Returns:
        output_path: Path to the folder of the output data
    """

    config = configparser.ConfigParser()
    config.read(config_path)

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY'] 
    output_path = config['OUT']['OUTPUT_PATH']

    return output_path


def print_status(module_name, message):
    """
    Description: Prints status with timestamp

    Arguments:
        module_name: Name of module
        message: desired message to print

    Returns:
        None
    """
    datetimenow = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('{} - {} - {}'.format(datetimenow, module_name, message))