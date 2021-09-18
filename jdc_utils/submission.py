"""Generate TSV files for submission to JDC"""

from urllib.request import urlopen
import json
import sys
from jdc_utils.dictionary import NodeDictionary
from jdc_utils.transforms import map as map_jdc
from jdc_utils.transforms import to_quarter

#manifest used for production deployment
MANIFEST_URL = 'https://raw.githubusercontent.com/uc-cdis/cdis-manifest/master/jcoin.datacommons.io/manifest.json'


class NodeSubmission(NodeDictionary):
    
    def __init__(self, type,manifest_url=MANIFEST_URL):
        super().__init__(manifest_url=MANIFEST_URL,type=type)

    def map_df(self,df,mapfile):
        data = df.copy()
        map_jdc(data, mapfile)
        data.insert(0,'type',self.type)
        self.unvalidated_data = data
        return self

    def add_submitter_ids(self,ids,parent_node=None):
        #TODO: replace_id function from dataforge here
        if parent_node:
            self.unvalidated_data[f"{parent_node}.submitter_id"] = ids
        else:
            self.unvalidated_data["submitter_id"] = ids
        return self

    def add_quarter(self,from_column='date_recruited'):
        self.unvalidated_data['quarter_recruited'] = to_quarter(
            self.unvalidated_data.date_recruited
            ).fillna('Not reported')
        return self

    def add_role_in_project(self,role):
        self.unvalidated_data['role_in_project'] = role
        return self

    def validate_df(self):
        cols = self.schema.columns.keys()
        node_cols = self.unvalidated_data.columns.isin(cols)
        node_data = self.unvalidated_data.loc[:,node_cols]
        self.validated_data = self.schema.validate(node_data)
        return self

    def to_tsv(self,file_dir,file_path):
        Path(file_dir).mkdir(parents=True, exist_ok=True)
        self.validated_data.to_csv(os.path.join(file_dir,file_name))

