import spacy
import yaml
from spacy.matcher import Matcher
import time
from functools import partial

class NounPhraseExtractor:
    def __init__(self, keep_longest=False):
        with open('config/config.yaml', mode='r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            npe_config = config['noun_phrase_extractor']
            self.model_name = npe_config['model_name']
            self.patterns = npe_config['noun_phrase_patterns']

        self.nlp = spacy.load(self.model_name, disable=['ner', 'textcat'])
        self.matcher = Matcher(self.nlp.vocab)
        self.matcher.add('NounPhrase', self.patterns)
        self.keep_longest = keep_longest
    
    def get_pos(self, text):
        doc = self.nlp(text)
        return [(token.text, token.pos_) for token in doc]
    
    def keep_longest_overlap(self, matches):
        matches = sorted(matches, key=lambda x: x[2] - x[1], reverse=True)
        results = []
        spans = []

        for match in matches:
            _, start, end = match
            if not any(start < s_end and end > s_start for s_start, s_end in spans):
                results.append(match)
                spans.append((start, end))
        return results

    def keep_valid_matches(self, matches):
        # 长度大于1
        results = []
        for match in matches:
            _, start, end = match
            if end - start > 1:
                results.append(match)
        return results

    def extract(self, text):
        doc = self.nlp(text)
        matches = self.matcher(doc)
        if self.keep_longest:
            matches = self.keep_longest_overlap(matches)
        
        matches = self.keep_valid_matches(matches)
        return list(set([doc[start:end].text for _, start, end in matches]))

    def extract_batch(self, texts, n_processes):
        # 使用pipe批量处理文档
        docs = self.nlp.pipe(texts, n_process=n_processes)

        results = []
        
        for doc in docs:
            matches = self.matcher(doc)
            if self.keep_longest:
                matches = self.keep_longest_overlap(matches)
            matches = self.keep_valid_matches(matches)
            results.append(list(set([doc[start:end].text for _, start, end in matches])))
        
        return results

def test_extraction_speed():
    extractor = NounPhraseExtractor(keep_longest=True)
    texts = [
        '本发明提供一种选择货物配送车型的方法及装置。其中，该方法包括：获取配送单中待运输货物的属性信息；基于所述待运输货物的属性信息和预设的空间空闲���数，确定初始的目标车型集合；基于降维分层和递归原则相结合的方式，对所述目标车型集合分别进行模拟装车验证，确定最终的目标车型。采用本发明公开的选择货物配送车型的方法，能够针对待运输货物择优推荐车型实现物流配送过程自动化管理，实现端到端服务，消除中间环节人为判断及确认过程，有效提升了配送效率，降低了配送成本，使得配送过程时效有显著提升。'
    ] * 32000

    # 测试不同进程数的性能
    for n_processes in [1, 2, 4, 8, 16, 32, 64]:
        start_time = time.time()
        extractor.extract_batch(texts, n_processes=n_processes)
        batch_time = time.time() - start_time
        print(f"Batch process time ({n_processes} processes): {batch_time:.2f} seconds")

    # 测试单文档处理性能
    start_time = time.time()
    for text in texts:
        extractor.extract(text)
    single_time = time.time() - start_time
    print(f"\nSingle process time: {single_time:.2f} seconds")

if __name__ == '__main__':
    # texts = [
    #     '本发明提供一种选择货物配送车型的方法及装置。其中，该方法包括：获取配送单中待运输货物的属性信息；基于所述待运输货物的属性信息和预设的空间空闲���数，确定初始的目标车型集合；基于降维分层和递归原则相结合的方式，对所述目标车型集合分别进行模拟装车验证，确定最终的目标车型。采用本发明公开的选择货物配送车型的方法，能够针对待运输货物择优推荐车型实现物流配送过程自动化管理，实现端到端服务，消除中间环节人为判断及确认过程，有效提升了配送效率，降低了配送成本，使得配送过程时效有显著提升。',
    #     '本发明提供了一种双屏蔽仪表电缆终端制作工艺，涉及双屏蔽仪表电缆制作技术领域，包括以下步骤：对双屏蔽仪表电缆开剥；对双屏蔽仪表电缆成对线芯进行热缩；对双屏蔽仪表电缆总屏蔽线芯进行热缩；对双屏蔽仪表电缆根部进行电缆头制作；对双屏蔽仪表电缆成对线芯开剥，并对分屏蔽线芯热缩；通过将分屏蔽与总屏蔽分别热缩，实现了无多点接地情况，起到防止静电感应和低频干扰的作用，极大提高了双屏蔽仪表电缆的抗干扰能力，缓解了现有技术中存在的传统的屏蔽电缆制作工艺是直接从电缆头进行开剥，开剥后直接进入线槽，进行绑扎接线，电缆头到接线端这段的电缆没有屏蔽保护，导致电缆屏蔽效果差的技术问题。',
    #     '本发明公开了一种轴向磁场电机及其组装方法，包括转子和定子，转子位于两个定子之间，转子的外周设置有转子壳体，两个定子上分别具有第一出线和第二出线，转子壳体上设置有过线槽，过线槽内设置有过桥线，过桥线、第一出线以及第二出线位于轴向磁场电机的轴线的同一侧，过桥线的两端分别与第一出线和第二出线焊接。在本发明中未使用接线盒和线鼻子，仅仅是将第一出线和第二出线与过桥线进行焊接，因此减少了接线所占用的空间，从而利于轴向磁场电机内部零部件的布局。另外，相较于现有技术中使用接线盒和线鼻子的方式，本发明中在过线槽内放置过桥线并将过桥线与第一出线和第二出线进行焊接的方式更简单，从而提高了接线效率。'
    # ]

    # extractor = NounPhraseExtractor(keep_longest=True)
    # noun_phrases = extractor.extract_batch(texts, n_processes=4)
    # print(noun_phrases)

    test_extraction_speed()