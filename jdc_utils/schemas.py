"""Easy access to latest versions of JCOIN schema"""

from types import SimpleNamespace

from frictionless import Schema

branch = "master"
schemadir = f"https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/{branch}/schemas/"
core_measures = SimpleNamespace(
    baseline=Schema(schemadir + "table-schema-baseline.json"),
    timepoints=Schema(schemadir + "table-schema-time-points.json"),
    staffbaseline=Schema(schemadir + "table-schema-staff-baseline.json"),
    stafftimepoints=Schema(schemadir + "table-schema-staff-time-points.json"),
)
