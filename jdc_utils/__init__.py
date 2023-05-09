import confuse

# frictionless plugins
from dataforge.frictionless import (
    frictionless_dataforgespss,
    frictionless_dataforgestata,
)

# general modules
from . import submission, transforms

# project specific builder classes
from .core_measures import CoreMeasures

# read config file import
config = confuse.Configuration("jdc-utils", __name__)
try:
    config.set_file("config.yaml")
except:
    pass
