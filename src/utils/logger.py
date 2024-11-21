import logging
import yaml
import os

def load_config(config_path='config/config.yaml'):
    with open(config_path, mode='r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config

config = load_config()
log_config = config['logging']

log_level = log_config.get('level', 'INFO')
log_level = getattr(logging, log_level.upper())

log_file = log_config.get('file', 'logs/app.log')
log_dir = os.path.dirname(log_file)
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level = log_level,
    format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(message)s'),
    handlers = [
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.debug('Debug message')
    logger.info('Info message')
    logger.warning('Warning message')
    logger.error('Error message')
    logger.critical('Critical message')