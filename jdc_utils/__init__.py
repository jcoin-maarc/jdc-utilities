from . import submission
from . import transforms
#frictionless plugins
from dataforge.frictionless import frictionless_dataforgespss
from dataforge.frictionless import frictionless_dataforgestata

import confuse

config = confuse.Configuration('jdc-utils',__name__)
try:
    config.set_file('config.yaml')
except:
    pass 