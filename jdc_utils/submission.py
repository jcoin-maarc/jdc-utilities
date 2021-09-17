"""Generate TSV files for submission to JDC"""

from urllib.request import urlopen
import json
import sys
from dictionary import build_schema

#manifest used for production deployment
MANIFEST_URL = 'https://raw.githubusercontent.com/uc-cdis/cdis-manifest/master/jcoin.datacommons.io/manifest.json'


class Node:
    
    def __init__(self, type):
        
        self.type = type
        
        manifest_json = json.loads(urlopen(MANIFEST_URL).read())
        dictionary_url = manifest_json['global']['dictionary_url']
        dictionary = json.loads(urlopen(dictionary_url).read())
        
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
    
    def to_tsv(self, df, path_or_buf, submitter_id=None, add_suffix=False,
               constants=None):
        
        data = df.copy()
        if constants:
            for c in constants:
                data[c] = constants[c]

        exclude = ['type','submitter_id']
        cols = [p for p in self.properties.keys()
                if p not in self.system_properties + exclude]
        if submitter_id:
            cols.append(submitter_id)
        data = data.loc[:,data.columns.isin(cols)]
        
        if submitter_id:
            data.set_index(submitter_id, inplace=True, verify_integrity=True)
        data.index.rename('submitter_id', inplace=True)
        
        if add_suffix:
            # Unfortunately add_suffix() doesn't apply here
            data.rename(lambda x: f'{x}_{self.type}', inplace=True)
        
        for link in self.links:
            if link['name'] in data.columns:
                data.rename(columns={link['name']:f"{link['name']}.submitter_id"},
                            inplace=True)
        
        data.insert(0, 'type', self.type)
        self.schema.validate(data)
        data.to_csv(path_or_buf, sep='\t')
