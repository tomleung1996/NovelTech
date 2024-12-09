import yaml
import ast
import pickle
from tqdm import tqdm
from src.utils.logger import logger
from src.db.database import Database
from src.db.data_fetcher import DataFetcher
from src.processing.noun_phrase_extractor import NounPhraseExtractor

class CurrentNounPhraseSetBuilder:
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            self.config = config
            self.db = Database()
            self.time_range = config['data_time_range']['current']
            self.data_fetcher = DataFetcher(config_path, self.time_range, self.db)
            self.noun_phrase_extractor = NounPhraseExtractor(keep_longest=True)
            self.list_output_path = config['output_paths']['current_noun_phrases']
    
    def extract_noun_phrases(self):
        results = []

        for batch in tqdm(self.data_fetcher.get_patent_ti_abs()):
            uuid_list = [uuid for uuid, title, abstract in batch]
            ti_list = [title for uuid, title, abstract in batch]
            abs_list = [abstract for uuid, title, abstract in batch]

            ti_np_list = self.noun_phrase_extractor.extract_batch(ti_list)
            abs_np_list = self.noun_phrase_extractor.extract_batch(abs_list)

            results = results + list(zip(uuid_list, ti_np_list, abs_np_list))

            with open(self.list_output_path, mode='a', encoding='utf-8') as f:
                for uuid, ti_np, abs_np in results:
                    f.write(f'{uuid}\t{ti_np}\t{abs_np}\n')
            
            results.clear()

        self.db.close_connection()

if __name__ == '__main__':
    builder = CurrentNounPhraseSetBuilder()

    logger.info('开始抽取当前专利（2014-2023）的名词短语……')
    builder.extract_noun_phrases()
    logger.info('抽取完成！')