import pandas as pd
import spacy
from file_processing import FileProcess

class SpacyProcessing:

    def __init__(self) -> None:
        self.list_text_pos = []
        self.list_text_ner = []
        self.list_sent_ner = []
        self.list_pos_nlp = []
        self.list_topics_ner = []
        self.list_nlp = []
        self.list_ner = []
        self.nlp = spacy.load("en_core_web_trf")
        self.file_process = FileProcess('Linus Tech Tips')

    def to_pos(self,data):
        nlp_data = self.nlp(''.join(data))
        pos_to_include = ['VERB','NOUN','PRON','ADJ','PROPN']
        for pos in nlp_data:
            if pos.pos_ in pos_to_include:
                self.list_pos_nlp.append(pos.pos_)
                self.list_text_pos.append(pos.text)
        data = {'pos_text':self.list_text_pos,'pos':self.list_pos_nlp}
        data = pd.DataFrame(data)
        return data
    
    def to_ner(self,data):
        ner_data = self.nlp(''.join(data))
        for ent in ner_data.ents:
            self.list_text_ner.append(ent.text)
            self.list_topics_ner.append(ent.label_)
        data = {'ner_text':self.list_text_ner,'ent':self.list_topics_ner}
        data_df = pd.DataFrame(data)
        return data_df
    
    def process_comments_nlp(self):
        data_json = self.file_process.open_S3_recent()
        data_comments = data_json['comments']
        for key in data_comments.keys():
            nlp_process = self.to_pos(data_comments[key]['comment_text'])
            nlp_process = nlp_process.value_counts().reset_index(name='pos_count')
            self.list_nlp.append(nlp_process)
            ner_process = self.to_ner(data_comments[key]['comment_text'])
            ner_process = ner_process.value_counts().reset_index(name='ner_count')
            self.list_ner.append(ner_process)
        spacy_data = {'pos_data':self.list_nlp,'ner_data':self.list_ner}
        return spacy_data

    def process_spacy(self):
        spacy_proc = self.process_comments_nlp()
        metadata = self.file_process.open_S3_recent_metadata()
        ner_data = 'ner_data'
        pos_data = 'pos_data'
        spacy_proc_ner = spacy_proc['ner_data']
        spacy_proc_pos = spacy_proc['pos_data']
        spacy_data_ner = pd.concat(spacy_proc_ner,
                                  keys=metadata['meta_keys'],
                                  names=metadata['meta_names'])
        spacy_data_pos = pd.concat(spacy_proc_pos,
                                  keys=metadata['meta_keys'],
                                  names=metadata['meta_names'])
        spacy_data_ner = pd.DataFrame(spacy_data_ner).reset_index().drop(columns='level_3')
        spacy_data_pos = pd.DataFrame(spacy_data_pos).reset_index().drop(columns='level_3')
        self.file_process.process_file(ner_data,spacy_data_ner)
        self.file_process.process_file(pos_data,spacy_data_pos)
        return
    
    def process_spacy_pos(self):
        spacy_proc = self.process_comments_nlp()
        metadata = self.file_process.open_S3_recent_metadata()
        pos_data = 'pos_data'
        spacy_proc_pos = spacy_proc['pos_data']
        spacy_data_pos = pd.concat(spacy_proc_pos,
                                  keys=metadata['meta_keys'],
                                  names=metadata['meta_names'])
        spacy_data_pos = pd.DataFrame(spacy_data_pos).reset_index().drop(columns='level_3')
        spacy_data_pos = self.file_process.process_file(pos_data,spacy_data_pos)
        return spacy_data_pos

    def process_spacy_ner(self):
        spacy_proc = self.process_comments_nlp()
        metadata = self.file_process.open_S3_recent_metadata()
        ner_data = 'ner_data'
        spacy_proc_ner = spacy_proc['ner_data']
        spacy_data_ner = pd.concat(spacy_proc_ner,
                                  keys=metadata['meta_keys'],
                                  names=metadata['meta_names'])
        spacy_data_ner = pd.DataFrame(spacy_data_ner).reset_index().drop(columns='level_3')
        spacy_data_ner =  self.file_process.process_file(ner_data,spacy_data_ner)
        return spacy_data_ner