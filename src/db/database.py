import psycopg
import yaml
from src.utils.logger import logger

class Database:
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            db_config = config['database']

            self.connection = psycopg.connect(
                dbname=db_config['db_name'],
                user=db_config['user'],
                password=db_config['password'],
                host=db_config['host'],
                port=db_config['port']
            )

    def fetch(self, query):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query)
                return cursor.fetchall()
            except psycopg.Error as e:
                logger.error(f'执行查询失败: {e}')
                print(f'执行查询失败: {e}')
                return None
    
    def close_connection(self):
        self.connection.close()