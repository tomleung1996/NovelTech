import yaml
import pandas as pd
from src.db.database import Database
from src.db.data_fetcher import DataFetcher
from src.utils.logger import logger

class EmergenceYearCalculator():
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            self.config = config

            self.db = Database()
            self.time_range = config['data_time_range']['current']
            self.data_fetcher = DataFetcher(config_path, self.time_range, self.db)

            self.increase_ratio_threshold = config['emergence_year_calculator']['increase_ratio_threshold']
            self.increase_span_threshold = config['emergence_year_calculator']['increase_span_threshold']

            logger.info('正在读取技术术语的倒排索引……')
            inverse_index_path = config['output_paths']['inverse_index']
            self.tech_noun_phrase_inverse_index_df = pd.read_csv(inverse_index_path, sep='\t', header=None, names=['tech_noun_phrase', 'uuid'])
            logger.info('读取完成')

            logger.info('正在获取各专利的申请年份……')
            self.patent_year_df = {
                'uuid': [],
                'year': []
            }

            for batch in self.data_fetcher.get_patent_appl_year():
                uuid_list = [uuid for uuid, year in batch]
                year_list = [year for uuid, year in batch]

                self.patent_year_df['uuid'].extend(uuid_list)
                self.patent_year_df['year'].extend(year_list)
            
            self.patent_year_df = pd.DataFrame(self.patent_year_df)
            logger.info('获取完成')

            self.tech_noun_phrase_inverse_index_df = self.tech_noun_phrase_inverse_index_df.merge(self.patent_year_df, on='uuid', how='inner')

            self.output_path = config['output_paths']['emergence_year']
    
    def calculate_emergence_year(self):
        # 兴起年定义：若在t年之后（含t年）的连续n年内，技术术语所对应的专利数量年均增长率大于等于阈值，则称t年为技术术语的兴起年
        logger.info('正在计算技术术语的兴起年……')
        tech_noun_phrase_annual_count = self.tech_noun_phrase_inverse_index_df.groupby(['tech_noun_phrase', 'year']).size().reset_index(name='count')
        # tech_noun_phrase_annual_count.sort_values(by=['tech_noun_phrase', 'year'], inplace=True)

        # 对于不连续的年份，需要填充缺失年份为0
        full_years = pd.Series(range(self.time_range['start_year'], self.time_range['end_year'] + 1), name='year')
        all_combinations = pd.MultiIndex.from_product([tech_noun_phrase_annual_count['tech_noun_phrase'].unique(), full_years], names=['tech_noun_phrase', 'year'])
        tech_noun_phrase_annual_count = tech_noun_phrase_annual_count.set_index(['tech_noun_phrase', 'year']).reindex(all_combinations, fill_value=0).reset_index()
        tech_noun_phrase_annual_count.sort_values(by=['tech_noun_phrase', 'year'], inplace=True)

        # 计算增长率
        tech_noun_phrase_annual_count['cumsum'] = tech_noun_phrase_annual_count.groupby('tech_noun_phrase')['count'].cumsum()
        tech_noun_phrase_annual_count['increase_ratio'] = tech_noun_phrase_annual_count.groupby('tech_noun_phrase')['cumsum'].pct_change()
        tech_noun_phrase_annual_count['increase_ratio'] = tech_noun_phrase_annual_count['increase_ratio'].fillna(0)
        # 将inf替换为0
        tech_noun_phrase_annual_count['increase_ratio'] = tech_noun_phrase_annual_count['increase_ratio'].replace([float('inf'), float('-inf')], 0)

        # 计算兴起年
        emergence_year_df = {
            'tech_noun_phrase': [],
            'emergence_year': []
        }

        for tech_noun_phrase, group in tech_noun_phrase_annual_count.groupby('tech_noun_phrase'):
            for i in range(len(group) - self.increase_span_threshold):
                # if all(group['increase_ratio'].iloc[i:i+self.increase_span_threshold] >= self.increase_ratio_threshold):
                if group['increase_ratio'].iloc[i:i+self.increase_span_threshold].mean() >= self.increase_ratio_threshold:
                    emergence_year_df['tech_noun_phrase'].append(tech_noun_phrase)
                    emergence_year_df['emergence_year'].append(group['year'].iloc[i])
                    break
            
            if tech_noun_phrase not in emergence_year_df['tech_noun_phrase']:
                emergence_year_df['tech_noun_phrase'].append(tech_noun_phrase)
                emergence_year_df['emergence_year'].append(None)
        
        emergence_year_df = pd.DataFrame(emergence_year_df)
        logger.info('计算完成')
        
        emergence_year_df.to_csv(self.output_path, sep='\t', index=False, header=None)

if __name__ == '__main__':
    calculator = EmergenceYearCalculator()
    calculator.calculate_emergence_year()