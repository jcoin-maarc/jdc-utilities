"""Easy access to latest versions of JCOIN schema"""
from frictionless import Schema

branch = "master"
schemadir = f"https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/{branch}/schemas/"
baseline = Schema(schemadir + "table-schema-baseline.json")
timepoints = Schema(schemadir + "table-schema-time-points.json")
staff_baseline = Schema(schemadir + "table-schema-staff-baseline.json")
staff_timepoints = Schema(schemadir + "table-schema-staff-time-points.json")
