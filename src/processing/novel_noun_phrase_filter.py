import yaml
import pickle
import ast
from tqdm import tqdm
from src.utils.logger import logger

class NovelNounPhraseFilter:
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            self.existing_noun_phrase_path = config['output_paths']['existing_noun_phrases']
            self.current_noun_phrases_path = config['output_paths']['current_noun_phrases']
            self.current_novel_noun_phrases_path = config['output_paths']['current_novel_noun_phrases']

            self.existing_noun_phrase_set = set()
            logger.info('正在统计现存名词短语……')
            with open(self.existing_noun_phrase_path, mode='r', encoding='utf-8') as f:
                for line in tqdm(f):
                    _, ti_np_list, abs_np_list = line.strip().split('\t')
                    ti_np_list = ast.literal_eval(ti_np_list)
                    abs_np_list = ast.literal_eval(abs_np_list)

                    self.existing_noun_phrase_set.update(ti_np_list)
                    self.existing_noun_phrase_set.update(abs_np_list)
            
            logger.info(f'现存名词短语共 {len(self.existing_noun_phrase_set)} 个')

            self.current_noun_phrase_dict = {}
            logger.info('正在建立当前名词短语字典……')
            with open(self.current_noun_phrases_path, mode='r', encoding='utf-8') as f:
                for line in tqdm(f):
                    uuid, ti_np_list, abs_np_list = line.strip().split('\t')
                    ti_np_list = ast.literal_eval(ti_np_list)
                    abs_np_list = ast.literal_eval(abs_np_list)

                    ti_abs_np_list = list(set(ti_np_list + abs_np_list))

                    self.current_noun_phrase_dict[uuid] = ti_abs_np_list

            logger.info(f'当前名词短语字典建立完成，共 {len(self.current_noun_phrase_dict)} 个专利')
    
    def filter_novel_noun_phrases(self):
        logger.info('正在过滤新颖名词短语……')
        for uuid, np_list in tqdm(self.current_noun_phrase_dict.items()):
            for np in np_list:
                if np in self.existing_noun_phrase_set:
                    np_list.remove(np)
        logger.info('过滤完成')

        logger.info('正在保存新颖名词短语……')
        with open(self.current_novel_noun_phrases_path, mode='w', encoding='utf-8') as f:
            for uuid, np_list in tqdm(self.current_noun_phrase_dict.items()):
                f.write(f'{uuid}\t{np_list}\n')
        logger.info('保存完成')

if __name__ == '__main__':
    filter = NovelNounPhraseFilter()
    filter.filter_novel_noun_phrases()
