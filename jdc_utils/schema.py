"""Easy access to latest versions of JCOIN schema"""

from types import SimpleNamespace
from frictionless import Schema

core_measures = SimpleNamespace(
    baseline = Schema('https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/pschumm/reorg/schemas/table-schema-baseline.json'),
    timepoints = Schema('https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/pschumm/reorg/schemas/table-schema-time-points.json')
)
