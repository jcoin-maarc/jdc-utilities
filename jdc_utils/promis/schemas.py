"""Easy access to latest versions of JCOIN schema"""
from frictionless import Schema

branch = "main"
schemadir = f"https://raw.githubusercontent.com/jcoin-maarc/jcoin-collab-projects/{branch}/schemas/promis/"
baseline = Schema(schemadir + "baseline.json")
timepoints_promis = Schema(schemadir + "timepoints_promis.json")
