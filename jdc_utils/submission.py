"""Generate TSV files for submission to JDC"""

from urllib.request import urlopen
import json
import sys
from jdc_utils.dictionary import build_schema,get_dictionary
from jdc_utils.transforms import map as map_jdc
from jdc_utils.transforms import to_quarter

#manifest used for production deployment
MANIFEST_URL = 'https://raw.githubusercontent.com/uc-cdis/cdis-manifest/master/jcoin.datacommons.io/manifest.json'


class Node:
    
    def __init__(self, type):
        
        self.type = type
        dictionary = get_dictionary(MANIFEST_URL) #note - full dictionary will be needed for node refs in future versions
        
        self.system_properties = dictionary[f'{type}.yaml']['systemProperties']
        self.properties = dictionary[f'{type}.yaml']['properties']
        self.links = dictionary[f'{type}.yaml']['links']
        self.required = dictionary[f'{type}.yaml']['required']
        self.unique_keys = dictionary[f'{type}.yaml']['uniqueKeys']

        self.schema = build_schema(
            self.properties,
            self.links,
            self.required,
            self.unique_keys,
            self.system_properties
        )

    def map_df(self,df,mapfile):
        data = df.copy()
        map_jdc(data, mapfile)
        data.insert(0,'type',self.type)
        self.unvalidated_data = data

    def add_submitter_ids(self,ids,parent_node=None):
        #TODO: replace_id function from dataforge here
        if parent_node:
            self.unvalidated_data[f"{parent_node}.submitter_id"] = ids
        else:
            self.unvalidated_data["submitter_id"] = ids

    def add_quarter(self,from_column='date_recruited'):
        self.unvalidated_data['quarter_recruited'] = to_quarter(
            self.unvalidated_data.date_recruited
            ).fillna('Not reported')

    def add_role_in_project(self,role):
        self.unvalidated_data['role_in_project'] = role

    def validate_df(self):
        cols = self.schema.columns.keys()
        node_cols = self.unvalidated_data.columns.isin(cols)
        node_data = self.unvalidated_data.loc[:,node_cols]
        self.validated_data = self.schema.validate(node_data)

    def to_tsv(self,file_dir,file_path):
        Path(file_dir).mkdir(parents=True, exist_ok=True)
        self.validated_data.to_csv(os.path.join(file_dir,file_name))

