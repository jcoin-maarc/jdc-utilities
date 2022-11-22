"""Easy access to latest versions of JCOIN schema"""

from frictionless import Schema
from typing import Dict 
def get_dict(dictionary):
    ''' 
    flexible helper function 
    to get dict from json/yaml if remote
    or any other formats frictionless.Schema supports

    (can be a path or dict)
    '''
    return Schema(dictionary).to_dict()

branch = 'master'
core_measures = SimpleNamespace(
    reserve = get_dict(f'https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/{branch}/encodings/encodings.yaml'),
    fields = get_dict(f'https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/{branch}/encodings/reserve_codes.yaml')
)

