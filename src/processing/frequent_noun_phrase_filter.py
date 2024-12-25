import yaml
import ast
import requests
from tqdm import tqdm
from src.utils.logger import logger
from collections import defaultdict

class FrequentNounPhraseFilter:
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            self.current_novel_noun_phrases_path = config['output_paths']['current_novel_noun_phrases']
            self.current_frequent_noun_phrases_path = config['output_paths']['current_frequent_noun_phrases']
            self.current_frequent_noun_phrases_translation_path = config['output_paths']['current_frequent_noun_phrases_translation']
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
            logger.info('正在统计新颖名词短语频次……')
            for np_list in tqdm(self.existing_noun_phrase_dict.values()):
                for np in np_list:
                    self.noun_phrase_freq_dict[np] += 1
            logger.info(f'新颖名词短语频次统计完成，共 {len(self.noun_phrase_freq_dict)} 个')

    def translate_noun_phrases(self, noun_phrase_list):
        url = 'https://api.deeplx.org/vchWZZoGEakqNqpoXTW8BxiLPWGAetTMlalnIpPysMI/translate'
        data = {
            'text': '\n'.join(noun_phrase_list),
            'source_lang': 'zh',
            'target_lang': 'en'
        }
        response = requests.post(url, json=data)
        translation = response.json()['data'].split('\n')
        return translation
        
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
        logger.info('保存完成')

        # 翻译
        logger.info('将高频名词短语翻译成英文……')
        with open(self.current_frequent_noun_phrases_translation_path, mode='a', encoding='utf-8') as f:
            # batch processing
            batch_size = 500
            np_list = list(filtered_noun_phrase_dict.keys())
            for i in tqdm(range(0, len(np_list), batch_size)):
                batch_np_list = np_list[i:i+batch_size]
                # 将np中的“所述”和“述”去掉
                translate_batch_np_list = [np.replace('所述', '').replace('述', '') for np in batch_np_list]
                translation = self.translate_noun_phrases(translate_batch_np_list)
                for np, eng_np in zip(batch_np_list, translation):
                    f.write(f'{np}\t{eng_np}\n')
        logger.info('翻译完成')

        return filtered_noun_phrase_dict

if __name__ == '__main__':
    filter = FrequentNounPhraseFilter()
    filter.filter_frequent_noun_phrases()