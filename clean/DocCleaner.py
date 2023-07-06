'''
Author: Aman
Date: 2023-04-10 15:53:34
Contact: cq335955781@gmail.com
LastEditors: Aman
LastEditTime: 2023-07-06 22:39:56
'''
from typing import List, Dict
from pathlib import Path
import os
import re


class DocCleaner:
    '''
    This is a multilingual document cleaner for CommonCrawl data cleaning.
    '''
    def __init__(self, dtwds_path='./'):
        self.dirty_word_list, self.dirty_word_list_effi = self.load_dirty_word_list(dtwds_path)

    def load_dirty_word_list(self, dirty_word_path='./'):
        '''
        Load the dirty word list.
        '''
        dirty_dir = Path(dirty_word_path) / 'List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words'
        if not dirty_dir.exists():
            raise ValueError(f'Please check the path of dirty word list: {dirty_dir}')
            exit(1)
        dirty_word_list = []
        for root, dirs, files in os.walk(dirty_dir):
            # this is for the order of the dirty words, so that the dirty words in more common languages are checked first
            files_head = ['en', 'zh', 'de', 'ja', 'fr', 'it', 'es', 'ar', 'th', 'ru', 'eo']
            files = files_head + [file for file in files if file not in ['README.md', 'LICENSE', 'USERS.md'] + files_head]
            for file in files:
                with open(os.path.join(root, file), 'r') as f:
                    dirty_word_list.extend(f.readlines())
        dirty_word_list = [word.strip() for word in dirty_word_list]
        # white_list = ['13.', '性']
        # dirty_word_list = [word for word in dirty_word_list if word not in white_list]

        ##########################################################################################
        ### Dedup - some dirty words a are included in other dirty words b, 
        ###   which will increase the complexity in the subsequent cleaning and searching.
        ### This part of the code is used to remove those words b, because 
        ###   when removing documents containing words a, those containing words b have been removed.
        ##########################################################################################
        dirty_word_dict = {} # key: word K, value: the subword of word K in the list with the minimum length
        for word in dirty_word_list:
            tmp_value = word
            for each in dirty_word_list:
                if each in word and len(each) < len(word): # get the minimum length subword
                    tmp_value = each
            dirty_word_dict[word] = tmp_value
        dirty_word_efficient = [value for key, value in dirty_word_dict.items()]
        dirty_word_efficient = ['{', '}', 'javascript', 'lorem ipsum'] + dirty_word_efficient
        # print(f'Loaded {len(dirty_word_efficient)} dirty words. {dirty_word_efficient[:20]}'); exit()
        
        return dirty_word_list, dirty_word_efficient

    def filter_documents(self, documents: List[Dict[str, str]]) -> List[Dict[str, str]]:
        '''
        This function filters out documents.
        '''
        # print(documents[0], len(documents))
        filtered_documents = []
        for doc in documents:
            if not self.passage_filtering(doc):
                filtered_documents.append(doc)
        return filtered_documents

    def passage_filtering(self, passage: Dict[str, str]) -> bool:
        '''
        This is for passage level filtering rules.
        Args:
            passage: a passage in a document.
        Returns:
            return True if the passage is filtered out.
        Efficiency:
            total: 340MB * 100
            # remove 1 remain - final: 20GB
            # remove 3&4 remain - final: 763MB
            # remove 5 remain - final: 311MB
            # remove 6 remain - final: 302MB
            # all - final: 300MB
        '''
        ###########################################
        raw_content = passage['raw_content']
        raw_content_length = len(raw_content)
        ### remove duplicated \n, space, tab, \r, \r\n
        ### However, this only works for about 1% of docs with low efficiency (few chars substitution), so not used.
        # raw_content_short = re.sub(r'[\n]+|\s{2,}|[\t]+|[\r]+|[\r\n]+', lambda match: match.group(0)[0], raw_content)
        
        ###########################################
        
        ### 1. filter out the passage with length less than 512
        if raw_content_length < 512:
            return True
        
        all_paras = [para for para in raw_content.split('\n') if para.strip()]
        ### 2. filter out the passage containing sentences less than 5
        if len(all_paras) <= 5:
            return True
        ### 3. filter out the passage containing short paras(<16) ratio more than 0.5
        if sum(1 for para in all_paras if len(para) < 16)/len(all_paras) > 0.5:
            return True
        
        ### 4. filter out the passage containing 'Lorem Ipsum', '{', '}' and dirty words
        lower_passage = raw_content.lower()
        if any(word in lower_passage for word in self.dirty_word_list_effi):
            return True
        
        ### 5. filter out the passage with ratio that starts with bullet point more than 0.9 and ends with ellipsis more than 0.3
        # bullet_point_count = sum(1 for para in all_paras if para.strip().startswith(('- ', '* ', '+ ', '• ', '– ', '— ', '⁃ ', '· ',
        #                              '◦ ', '▪ ', '▫ ', '▶ ', '▸ ', '▹ ', '► ', '▻ ', '▼ ', '▽ ', '▾ ', '▿ ', '◀ ', '◁ ', '◂ ', '◃ ',
        #                              '◄ ', '◅ ', '◆ ', '◇ ', '◈ ', '◉ ', '◊ ', '○ ', '◌ ', '◍ ', '◎ ', '● ')))
        bullet_point_count = sum(1 for para in all_paras if para.strip().startswith((\
            '-', '–', '—', '⁃', '*', '+', '●', '•', '·', '◦', '▪', '▫', '▶', '▸', '▹', '►', '▼', '▽', '▾', '▿'))) 
        ellipsis_count = sum(1 for para in all_paras if para.strip().endswith(('...', '…')))
        if bullet_point_count / len(all_paras) > 0.9 or ellipsis_count / len(all_paras) > 0.3:
            return True

        ### 6. filter out the passage with ritio of '#' or '...' more than 0.1
        short_content = ' '.join(all_paras)
        if short_content.count('#') / raw_content_length > 0.1 or \
            short_content.count('…') / raw_content_length > 0.1 or \
            short_content.count('...') / raw_content_length > 0.1:
            return True
        
        return False
        
    def document_cleaning(self, document: Dict[str, str]) -> Dict[str, str]:
        '''
        This function cleans ONE document.
        '''
        document['raw_content'] = self.content_cleaning(document['raw_content'])
        if len([x for x in document['raw_content'].split('\n') if x.strip()]) <= 3:
            return None
        return document

    def clean_documents(self, documents: List[Dict[str, str]]) -> List[Dict[str, str]]:
        '''
        This function cleans documents.
        '''
        cleaned_documents = []
        for doc in documents:
            cleaned_doc = self.document_cleaning(doc)
            if cleaned_doc is not None:
                cleaned_documents.append(cleaned_doc)
        return cleaned_documents
    
    def content_cleaning(self, content: str) -> str:
        '''
        This function cleans the content.
        '''
        # content_length = len(content)
        ### get all paras and all words
        all_paras = [para for para in content.split('\n')]
        
        cleaned_content = ''
        multi_next_row_count = 0
        for para in all_paras:
            if para == '\n':
                if multi_next_row_count > 1: # multi rows of \n => max 2
                    continue
                else:
                    cleaned_content += para
                    multi_next_row_count += 1
            short_para = para.strip()
            
            #######################################################

            ### 1. remove paras that do not end with certain punctuations
            if not short_para or short_para[-1] not in ('.', '。', '!', '！', '?', '？', '"', '”', "'", '’', '…'):
                continue
            
            ### 2. remove paras with length less than 16
            if len(short_para) < 16:
                continue

            ### 3. remove paras that contain mess code
            if '�' in para:
                para = para.replace('�', '')

            ### 4. remove paras with less than 3 words
            word_list= list(filter(None, map(str.strip, para.split())))
            if len(word_list) <= 3:
                continue
            
            ### 5. remove paras with average word length not in 3 to 10
            avg_word_len = sum(len(word) for word in word_list)/len(word_list)
            if avg_word_len < 3 or avg_word_len > 10:
                continue

            ### 6. remove paras with messy code
            para = re.sub(r"[\x00-\x1F\x7F]", "", para)
            para = re.sub(r"[\u2002\u2003\u3000]", " ", para)
            
            #######################################################
            cleaned_content += para + '\n'
            multi_next_row_count = 0
        
        return cleaned_content

