import confuse

# frictionless plugins
from dataforge.frictionless import (
    frictionless_dataforgespss,
    frictionless_dataforgestata,
)

from . import submission, transforms

config = confuse.Configuration("jdc-utils", __name__)
try:
    config.set_file("config.yaml")
except:
    pass
