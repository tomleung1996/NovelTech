import yaml
from tqdm import tqdm
from src.utils.logger import logger
from src.db.database import Database
from src.db.data_fetcher import DataFetcher

class HighlyCitedPatentFilter():
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            self.config = config['highly_cited_patent_filter']
            self.time_range = config['data_time_range']['current']
            self.db = Database()
            self.data_fetcher = DataFetcher(config_path, self.time_range, self.db)
            self.citation_threshold = self.config['citation_threshold']
            self.output_path = config['output_paths']['current_highly_cited_patents']
    
    def filter_highly_cited_patents(self):
        results = []

        for batch in tqdm(self.data_fetcher.get_patent_citation_count()):
            uuid_list = [uuid for uuid, citation_count in batch]
            citation_count_list = [citation_count for uuid, citation_count in batch]

            for uuid, citation_count in zip(uuid_list, citation_count_list):
                if citation_count >= self.citation_threshold:
                    results.append((uuid, citation_count))

            with open(self.output_path, mode='a', encoding='utf-8') as f:
                for uuid, citation_count in results:
                    f.write(f'{uuid}\t{citation_count}\n')
            
            results.clear()

        self.db.close_connection()

if __name__ == '__main__':
    filter = HighlyCitedPatentFilter()
    logger.info('正在过滤高被引专利……')
    filter.filter_highly_cited_patents()
    logger.info('过滤完成！')