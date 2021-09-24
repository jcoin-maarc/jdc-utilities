"""Generate TSV files for submission to JDC"""

from urllib.request import urlopen
import json
import sys
from jdc_utils.dictionary import NodeDictionary
from jdc_utils.transforms import map as map_jdc
from jdc_utils.transforms import to_quarter
from pathlib import Path
import os

#manifest used for production deployment
MANIFEST_URL = 'https://raw.githubusercontent.com/uc-cdis/cdis-manifest/master/jcoin.datacommons.io/manifest.json'


class NodeSubmission(NodeDictionary):
    
    def __init__(self, type,manifest_url=MANIFEST_URL):
        super().__init__(manifest_url=MANIFEST_URL,type=type)

    def map_df(self,df,mapfile=None):
        data = df.copy()
        if mapfile:
            map_jdc(data, mapfile)
        if 'type' not in data.columns:
            data.insert(0,'type',self.type)
        self.unvalidated_data = data
        return self

    def add_submitter_ids(self,ids,parent_node=None,is_index=True):
        #TODO: replace_id function from dataforge here?
        if parent_node:
            col_name = f"{parent_node}.submitter_id"
        else:
            col_name = "submitter_id"
        
        if is_index and not parent_node: #parent nodes cant be index
            self.unvalidated_data.index = ids
            self.unvalidated_data.index.name = col_name
        else:
            self.unvalidated_data[col_name] = ids
        return self

    def add_quarter(self,from_column='date_recruited'):
        #TODO: integrate as a tranform into schema?
        self.unvalidated_data['quarter_recruited'] = to_quarter(
            self.unvalidated_data[from_column]
            ).fillna('Not reported').astype(str)
        return self

    def add_role_in_project(self,role):
        self.unvalidated_data['role_in_project'] = role
        return self

    def validate_df(self):
        self.validated_data = self.schema.validate(self.unvalidated_data)
        return self.validated_data

    def to_tsv(self,file_dir,file_name,index=True):
        Path(file_dir).mkdir(parents=True, exist_ok=True)
        self.validated_data.to_csv(os.path.join(file_dir,file_name),index=index)

