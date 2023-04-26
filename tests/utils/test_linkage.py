"""
fuzzy matching to support
(1) variable name linkage/harmonization
and (2) value linkage from frictionless validation report
"""
import json
from pathlib import Path

from jdc_utils.schema import core_measures
from thefuzz import process

# read in and flatten core measure schemas
cdes = list(core_measures.baseline.fields)
cdes += list(core_measures.timepoints.fields)

# read in source data elements
sourcepath = (
    "C:/Users/kranz-michael/projects/jcoin-nyu/"
    "mappings/flattened_variables_and_values.json"
)
source = json.loads(Path(sourcepath).read_text())


#
def read_common_data_elements():
    pass


def read_variable_level_metadata():
    pass


def match_descriptions():
    pass


def match_variable_values():
    pass


def detect_error():
    pass


def map_error():
    pass


def write_mapping():
    pass
