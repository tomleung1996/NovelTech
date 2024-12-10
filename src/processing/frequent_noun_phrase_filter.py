import yaml
import ast
from tqdm import tqdm
from src.utils.logger import logger
from collections import defaultdict

class FrequentNounPhraseFilter:
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            self.current_novel_noun_phrases_path = config['output_paths']['current_novel_noun_phrases']
            self.current_frequent_noun_phrases_path = config['output_paths']['current_frequent_noun_phrases']
            self.min_doc_freq = config['frequent_noun_phrase_filter']['min_doc_freq']

            self.existing_noun_phrase_dict = {}
            logger.info('正在统计现存的新颖名词短语……')
            with open(self.current_novel_noun_phrases_path, mode='r', encoding='utf-8') as f:
                for line in tqdm(f):
                    uuid, np_list = line.strip().split('\t')
                    np_list = ast.literal_eval(np_list)

                    self.existing_noun_phrase_dict[uuid] = np_list
            logger.info(f'现存的新颖名词短语共 {len(self.existing_noun_phrase_dict)} 个专利')

            self.noun_phrase_freq_dict = defaultdict(int)
            logger.info('正在统计名词短语频次……')
            for np_list in tqdm(self.existing_noun_phrase_dict.values()):
                for np in np_list:
                    self.noun_phrase_freq_dict[np] += 1
            logger.info(f'名词短语频次统计完成，共 {len(self.noun_phrase_freq_dict)} 个名词短语')
        
    def filter_frequent_noun_phrases(self):
        min_doc_freq = self.min_doc_freq

        logger.info('正在过滤低频名词短语……')
        filtered_noun_phrase_dict = {}
        for np, freq in tqdm(self.noun_phrase_freq_dict.items()):
            if freq >= min_doc_freq:
                filtered_noun_phrase_dict[np] = freq
        logger.info(f'过滤完成，共 {len(filtered_noun_phrase_dict)} 个符合要求的名词短语')

        # 排序
        filtered_noun_phrase_dict = dict(sorted(filtered_noun_phrase_dict.items(), key=lambda x: x[1], reverse=True))

        logger.info('正在保存高频名词短语……')
        with open(self.current_frequent_noun_phrases_path, mode='w', encoding='utf-8') as f:
            for np, freq in tqdm(filtered_noun_phrase_dict.items()):
                f.write(f'{np}\t{freq}\n')

        return filtered_noun_phrase_dict

if __name__ == '__main__':
    filter = FrequentNounPhraseFilter()
    filter.filter_frequent_noun_phrases()