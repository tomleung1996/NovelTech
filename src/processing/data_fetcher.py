from src.db.database import Database
import yaml

class DataFetcher:
    def __init__(self, config_path, db: Database):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            fetch_config = config['data_fetcher']

            self.db = db
            self.appl_main_table = fetch_config['appl_main_table']
            self.appl_title_table = fetch_config['appl_title_table']
            self.appl_abstract_table = fetch_config['appl_abstract_table']
            self.appl_title_text_field = fetch_config['appl_title_text_field']
            self.appl_abstract_text_field = fetch_config['appl_abstract_text_field']
            self.patent_type = fetch_config['patent_type']
            self.application_start_year = fetch_config['application_start_year']
            self.application_end_year = fetch_config['application_end_year']
            self.application_year_field = fetch_config['application_year_field']
            self.not_null_field = fetch_config['not_null_field']
            self.row_limit = fetch_config['row_limit']
            self.order_by = fetch_config['order_by']
    
    def get_valid_record_count(self):
        query = f'''
            SELECT
                COUNT(*)
            FROM
                {self.appl_main_table}
            WHERE
                patent_type = '{self.patent_type}'
            AND
                CAST({self.application_year_field} AS INT) BETWEEN {self.application_start_year} AND {self.application_end_year}
            AND
                {self.not_null_field} IS NOT NULL
        '''
        return self.db.fetch(query)
    

    def get_patent_ti_abs(self):
        query = f'''
            SELECT
                m.appl_id,
                t.{self.appl_title_text_field},
                a.{self.appl_abstract_text_field}
            FROM
                {self.appl_main_table} m
            JOIN
                {self.appl_title_table} t
            ON
                m.appl_id = t.appl_id
            JOIN
                {self.appl_abstract_table} a
            ON
                m.appl_id = a.appl_id
            WHERE
                m.patent_type = '{self.patent_type}'
            AND
                CAST(m.{self.application_year_field} AS INT) BETWEEN {self.application_start_year} AND {self.application_end_year}
            AND
                m.{self.not_null_field} IS NOT NULL
            ORDER BY
                {self.order_by} DESC
            LIMIT
                {self.row_limit}
        '''
        return self.db.fetch(query)

if __name__ == '__main__':
    fetcher = DataFetcher('config/config.yaml', Database())
    result = fetcher.get_valid_record_count()
    print(result)