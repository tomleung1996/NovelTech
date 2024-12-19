# 为最后剩余的技术术语建立对高被引专利的倒排索引
import yaml
import ast
from src.utils.logger import logger
from tqdm import tqdm

class InverseIndexBuilder():
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            self.config = config
            
            logger.info('正在加载技术术语……')
            current_tech_noun_phrases_path = config['output_paths']['current_tech_noun_phrases']
            self.current_tech_noun_phrases = set()
            with open(current_tech_noun_phrases_path, mode='r', encoding='utf-8') as f:
                for line in tqdm(f):
                    np, _ = line.strip().split('\t')
                    self.current_tech_noun_phrases.add(np)
            logger.info(f'技术术语加载完成，共 {len(self.current_tech_noun_phrases)} 个')

            black_list = set(self.config['inverse_index_builder']['black_list'])
            self.current_tech_noun_phrases = self.current_tech_noun_phrases - black_list
            
            logger.info('正在加载高被引专利……')
            current_highly_cited_patents_path = config['output_paths']['current_highly_cited_patents']
            self.current_highly_cited_patents = set()
            with open(current_highly_cited_patents_path, mode='r', encoding='utf-8') as f:
                for line in tqdm(f):
                    uuid, _ = line.strip().split('\t')
                    self.current_highly_cited_patents.add(uuid)
            logger.info(f'高被引专利加载完成，共 {len(self.current_highly_cited_patents)} 个')
            
            logger.info('正在加载高被引专利的名词短语……')
            current_novel_noun_phrases_path = config['output_paths']['current_novel_noun_phrases']
            self.current_highly_cited_patent_to_np = {}
            with open(current_novel_noun_phrases_path, mode='r', encoding='utf-8') as f:
                for line in tqdm(f):
                    uuid, np_list = line.strip().split('\t')
                    np_list = ast.literal_eval(np_list)
                    if uuid in self.current_highly_cited_patents:
                        self.current_highly_cited_patent_to_np[uuid] = np_list
            logger.info(f'高被引专利的名词短语加载完成，共 {len(self.current_highly_cited_patent_to_np)} 个')

            self.output_path = config['output_paths']['inverse_index']
    
    def build_inverse_index(self):
        inverse_index = {}
        
        logger.info('正在为技术术语建立倒排索引……')
        for uuid, np_list in tqdm(self.current_highly_cited_patent_to_np.items()):
            for np in np_list:
                if np in self.current_tech_noun_phrases:
                    if np not in inverse_index:
                        inverse_index[np] = set()
                    inverse_index[np].add(uuid)
        logger.info(f'倒排索引建立完成')
        
        logger.info('正在保存倒排索引……')
        with open(self.output_path, mode='w', encoding='utf-8') as f:
            for np, uuid_set in inverse_index.items():
                for uuid in uuid_set:
                    f.write(f'{np}\t{uuid}\n')
        logger.info(f'倒排索引保存完成')

if __name__ == '__main__':
    builder = InverseIndexBuilder()
    builder.build_inverse_index()