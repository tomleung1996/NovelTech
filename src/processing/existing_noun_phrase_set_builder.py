import yaml
import ast
import pickle
from tqdm import tqdm
from src.utils.logger import logger
from src.db.database import Database
from src.db.data_fetcher import DataFetcher
from src.processing.noun_phrase_extractor import NounPhraseExtractor

class ExistingNounPhraseSetBuilder:
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            self.config = config
            self.db = Database()
            self.time_range = config['data_time_range']['existing']
            self.data_fetcher = DataFetcher(config_path, self.time_range, self.db)
            self.noun_phrase_extractor = NounPhraseExtractor(keep_longest=True)
            self.list_output_path = config['output_paths']['existing_noun_phrases']
            self.set_output_path = config['output_paths']['existing_noun_phrase_set']
    
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
    
    def build_noun_phrase_set(self):
        noun_phrase_set = set()

        with open(self.list_output_path, mode='r', encoding='utf-8') as f:
            for line in tqdm(f):
                _, ti_np_list, abs_np_list = line.strip().split('\t')
                ti_np_list = ast.literal_eval(ti_np_list)
                abs_np_list = ast.literal_eval(abs_np_list)

                noun_phrase_set.update(ti_np_list)
                noun_phrase_set.update(abs_np_list)
        
        with open(self.set_output_path, mode='wb') as f:
            pickle.dump(noun_phrase_set, f)

if __name__ == '__main__':
    builder = ExistingNounPhraseSetBuilder()

    # logger.info('开始抽取过去专利（1985-2013）的名词短语……')
    # builder.extract_noun_phrases()
    # logger.info('抽取完成！')

    logger.info('开始构建过去专利（1985-2013）的名词短语集合（set）……')
    builder.build_noun_phrase_set()
    logger.info('构建完成！')