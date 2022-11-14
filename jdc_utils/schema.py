"""Easy access to latest versions of JCOIN schema"""

from types import SimpleNamespace
from frictionless import Schema

branch = 'master'
core_measures = SimpleNamespace(
    baseline = Schema(f'https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/{branch}/schemas/table-schema-baseline.json'),
    timepoints = Schema(f'https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/{branch}/schemas/table-schema-time-points.json')
)
