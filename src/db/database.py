import psycopg
import yaml
from src.utils.logger import logger

class Database:
    def __init__(self, config_path='config/config.yaml'):
        try:
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
        except (psycopg.Error, FileNotFoundError, KeyError) as e:
            logger.error(f'数据库连接失败: {e}')
            self.connection = None

    def fetch(self, query):
        if not self.connection:
            logger.error('数据库连接未建立')
            return None

        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query)
                return cursor.fetchall()
            except psycopg.Error as e:
                logger.error(f'执行查询失败: {e}')
                return None
    
    def close_connection(self):
        if self.connection:
            self.connection.close()
        else:
            logger.error('数据库连接未建立或已关闭')

if __name__ == '__main__':
    db = Database()
    result = db.fetch('SELECT * FROM cnipa_appl LIMIT 10')
    db.close_connection()
    for row in result:
        print(row)