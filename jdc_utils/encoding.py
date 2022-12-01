"""Easy access to latest versions of JCOIN schema"""

from frictionless import Schema
from typing import Dict
from types import SimpleNamespace
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
    fields = get_dict(f'https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/{branch}/encodings/encodings.yaml'),
    reserve = get_dict(f'https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/{branch}/encodings/reserve_codes.yaml')
)

