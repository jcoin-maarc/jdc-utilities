"""Latest version of SPSS/Stata encodings (value labels)"""

import requests

branch = "master"
url = f"https://raw.githubusercontent.com/jcoin-maarc/JCOIN-Core-Measures/{branch}/encodings/"
fields = requests.get(url + "encodings.yaml").json()
reserve = requests.get(url + "reserve_codes.yaml").json()
