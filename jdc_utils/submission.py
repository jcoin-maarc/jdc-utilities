"""Generate TSV files for submission to JDC"""

from urllib.request import urlopen
import json
import sys

# TODO Replace with version-free link to current dictionary
DICTIONARY_URL = 'https://dictionary-artifacts.s3.amazonaws.com/jcoin_datadictionary/1.1.1/schema.json'

class Node:
    
    def __init__(self, type):
        
        self.type = type
        
        response = urlopen(DICTIONARY_URL)
        dictionary = json.loads(response.read())
        
        self.system_properties = dictionary[f'{type}.yaml']['systemProperties']
        self.properties = dictionary[f'{type}.yaml']['properties']
        self.links = dictionary[f'{type}.yaml']['links']
        self.required = dictionary[f'{type}.yaml']['required']
    
    def _validate(self, df):
        
        exclude = ['type','submitter_id']
        properties = [p for p in self.properties.keys()
                      if p not in self.system_properties + exclude]
        required = [p for p in self.required if p not in exclude]
        
        for property in properties:
            if property in required:
                if property not in df:
                    sys.exit(f'Column {property} required but not found')
                elif df[property].isnull().values.any():
                    sys.exit(f'Property {property} required but missing in one or more cases')
            
            if property in df and 'enum' in self.properties[property]:
                valid = set(self.properties[property]['enum'])
                if not set(df[property].dropna().unique()).issubset(valid):
                    invalid = set(df[property].dropna().unique()) - valid
                    sys.exit(f'Value(s) {invalid} not valid for {property}')
    
    def to_tsv(self, df, path_or_buf, submitter_id=None, add_suffix=False,
               constants=None):
        
        data = df.copy()
        if constants:
            for c in constants:
                data[c] = constants[c]
        self._validate(data)
        
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
        data.to_csv(path_or_buf, sep='\t')
