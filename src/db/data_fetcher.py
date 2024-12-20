from src.db.database import Database
import yaml

class DataFetcher:
    def __init__(self, config_path, time_range: dict, db: Database):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            fetch_config = config['data_fetcher']

            self.db = db
            self.appl_main_table = fetch_config['appl_main_table']
            self.appl_title_table = fetch_config['appl_title_table']
            self.appl_abstract_table = fetch_config['appl_abstract_table']
            self.appl_reference_table = fetch_config['appl_reference_table']
            self.appl_title_text_field = fetch_config['appl_title_text_field']
            self.appl_abstract_text_field = fetch_config['appl_abstract_text_field']
            self.patent_type = fetch_config['patent_type']
            self.application_start_year = time_range['start_year']
            self.application_end_year = time_range['end_year']
            self.application_year_field = fetch_config['application_year_field']
            self.not_null_field = fetch_config['not_null_field']
            self.row_limit = fetch_config['row_limit']
            self.offset = fetch_config['offset']
            self.order_by = fetch_config['order_by']
            self.batch_size = fetch_config['batch_size']
    
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
                m.uuid,
                COALESCE(t.{self.appl_title_text_field}, ''),
                COALESCE(a.{self.appl_abstract_text_field}, '')
            FROM
                {self.appl_main_table} m
            JOIN
                {self.appl_title_table} t
            ON
                m.uuid = t.uuid
            JOIN
                {self.appl_abstract_table} a
            ON
                m.uuid = a.uuid
            WHERE
                m.patent_type = '{self.patent_type}'
            AND
                CAST(m.{self.application_year_field} AS INT) BETWEEN {self.application_start_year} AND {self.application_end_year}
            AND
                m.{self.not_null_field} IS NOT NULL
            ORDER BY
                {self.order_by} DESC
            OFFSET
                {self.offset}
            LIMIT
                {self.row_limit}
        '''
        return self.db.fetch(query, self.batch_size)
    
    def get_patent_citation_count(self):
        query = f'''
            SELECT 
                t1."uuid", 
                COALESCE(t2.n_cits, 0) AS n_cits
            FROM 
                {self.appl_main_table} AS t1
            LEFT JOIN (
                SELECT 
                    CONCAT(apl_country_code, apl_appl_no, apl_kind) AS pub_id, 
                    COUNT(*) AS n_cits
                FROM 
                    {self.appl_reference_table}
                WHERE 
                    apl_country_code = 'CN'
                GROUP BY 
                    pub_id
            ) AS t2
                ON t1.pub_id = t2.pub_id
            WHERE 
                t1.patent_type = '{self.patent_type}'
            AND
                CAST(t1.{self.application_year_field} AS INT) BETWEEN {self.application_start_year} AND {self.application_end_year}
            AND
                t1.{self.not_null_field} IS NOT NULL
            ORDER BY 
                n_cits DESC
            OFFSET
                {self.offset}
            LIMIT
                {self.row_limit}
        '''
        return self.db.fetch(query, self.batch_size)
    
    def get_patent_appl_year(self):
        query = f'''
            SELECT
                uuid,
                CAST({self.application_year_field} AS INT)
            FROM
                {self.appl_main_table}
            WHERE
                patent_type = '{self.patent_type}'
            AND
                CAST({self.application_year_field} AS INT) BETWEEN {self.application_start_year} AND {self.application_end_year}
            AND
                {self.not_null_field} IS NOT NULL
            ORDER BY
                {self.order_by} DESC
            OFFSET
                {self.offset}
            LIMIT
                {self.row_limit}
        '''
        return self.db.fetch(query, self.batch_size)
        

if __name__ == '__main__':
    fetcher = DataFetcher('config/config.yaml', {'start_year':2014, 'end_year':2023}, Database())
    result = fetcher.get_patent_appl_year()
    print(next(result))