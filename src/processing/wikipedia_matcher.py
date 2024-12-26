import wikipedia
import yaml
import pywikibot
import editdistance
from tqdm import tqdm
from src.utils.logger import logger
from pywikibot import Page
# supress warnings
import warnings
warnings.filterwarnings("ignore")

class WikipediaMatcher:
    def __init__(self, config_path='config/config.yaml'):
        self.config = yaml.safe_load(open(config_path))
        self.current_frequent_noun_phrase_translation_path = self.config['output_paths']['current_frequent_noun_phrases_translation']
        self.edit_distance_threshold = self.config['wikipedia_matcher']['edit_distance_threshold']
        self.lang = self.config['wikipedia_matcher']['lang']
        self.site = pywikibot.Site(self.lang, 'wikipedia')

        self.white_list = self.config['wikipedia_matcher']['white_list']
        self.black_list = self.config['wikipedia_matcher']['black_list']
        self.current_frequent_noun_phrase_list = []
        self.current_frequent_noun_phrase_translation_list = []
        with open(self.current_frequent_noun_phrase_translation_path, 'r') as file:
            for line in file:
                self.current_frequent_noun_phrase_list.append(line.strip().split('\t')[0])
                self.current_frequent_noun_phrase_translation_list.append(line.strip().split('\t')[1])
        
        logger.info(f'载入待查询的高频新颖名词短语，共{len(self.current_frequent_noun_phrase_translation_list)}个')

        self.output_path = self.config['output_paths']['current_tech_noun_phrases']
    
    def get_edit_distance(self, s1, s2):
        return editdistance.eval(s1, s2)
    
    def is_valid_sections(self, sections):
        # 检查是否有黑名单关键词
        for title in sections:
            for black_keyword in self.black_list:
                if black_keyword.lower() in title.lower():
                    return False
        # 检查是否有白名单关键词
        for title in sections:
            for white_keyword in self.white_list:
                if white_keyword.lower() in title.lower().split(' ') or title.lower() in white_keyword.lower():
                    return True
        return False

    def match(self, np):
        try:
            search_results = list(self.site.search(np, total=1))
            if search_results:
                title = search_results[0].title()
                abstract = ''

                page = pywikibot.Page(self.site, title)
                
                if page.exists():
                    # 获取摘要
                    # summary_request = pywikibot.data.api.Request(
                    #     site=self.site,
                    #     action='query',
                    #     prop='extracts',
                    #     exintro=True,
                    #     explaintext=True,
                    #     titles=title
                    # )
                    # summary_data = summary_request.submit()
                    # pageid = next(iter(summary_data['query']['pages']))
                    # abstract = summary_data['query']['pages'][pageid].get('extract', '')
                
                    # 检查标题和np是否一致或标题被np所包含，并且编辑距离小于给定值，否则返回False。采用if not
                    if not(title.lower() == np.lower() or (np.lower() in title.lower() and self.get_edit_distance(np, title) <= self.edit_distance_threshold)):
                        return False
                    
                    # 获取目录
                    sections_request = pywikibot.data.api.Request(
                        site=self.site,
                        action='parse',
                        page=title,
                        prop='sections'
                    )
                    sections_data = sections_request.submit()
                    sections = sections_data['parse']['sections']
                    sections = [section['line'] for section in sections]

                    # 匹配白名单和黑名单
                    return self.is_valid_sections(sections)

                return False
            
            return False
        except Exception as e:
            logger.error(f'查询{np}时出错：{e}')
            return False
    
    def get_tech_noun_phrase_list(self):
        logger.info('开始匹配技术名词短语……')
        for i, np in tqdm(enumerate(self.current_frequent_noun_phrase_translation_list)):
            if self.match(np):
                with open(self.output_path, 'a') as file:
                    file.write(f'{self.current_frequent_noun_phrase_list[i]}\t{np}\n')


if __name__ == '__main__':
    wikipedia_matcher = WikipediaMatcher()
    wikipedia_matcher.get_tech_noun_phrase_list()