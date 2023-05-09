"""Latest version of SPSS/Stata encodings (value labels)"""

import requests
import yaml

branch = "master"
url = f"https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/{branch}/encodings/"
fields = yaml.safe_load(requests.get(url + "encodings.yaml").content)
reserve = yaml.safe_load(requests.get(url + "reserve_codes.yaml").content)
