import toml
import os
import importlib.resources as pkg_resources

### Default Context
default = "cnf"

### Load Configuration from Files
def load_config(default_config_path, user_config_path=None):
    # Load the default configuration file
    with pkg_resources.open_text('gbd_tools', default_config_path) as f:
        config = toml.load(f)
    
    # If a user configuration file is provided, load it and update the config
    if user_config_path and os.path.exists(user_config_path):
        with open(user_config_path, 'r') as f:
            user_config = toml.load(f)
        config.update(user_config)
    
    return config

### Convert ConfigParser to Dictionary
def config_to_dict(config):
    config_dict = {}
    for context, details in config['contexts'].items():
        config_dict[context] = {
            "description": details["description"],
            "suffix": details["suffix"],
            "idfunc": globals()[details["idfunc"]],
        }
    return config_dict

### Paths to Configuration Files
default_config_path = "default_config.toml"
user_config_path = "user_config.toml"  # Adjust this path as needed

### Load and Convert Configuration
config_parser = load_config(default_config_path, user_config_path)
config = config_to_dict(config_parser)