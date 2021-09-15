import pandera as pa
from pandera.typing import Series

from urllib.request import urlopen
import json
import sys

from pandera.io import from_frictionless_schema


#manifest used for production deployment
MANIFEST_URL = 'https://raw.githubusercontent.com/uc-cdis/cdis-manifest/master/jcoin.datacommons.io/manifest.json'
manifest_json = json.loads(urlopen(MANIFEST_URL).read())
dictionary_url = manifest_json['global']['dictionary_url']
dictionary = json.loads(urlopen(dictionary_url).read())
demographic_json = dictionary['demographic.yaml']
props_json = dictionary['demographic.yaml']['properties']

class Schema(pa.SchemaModel):

    participant.submitter_id: Series[str] = pa.Field()
    gender_identity: Series[str] = pa.Field(isin=)
    hispanic: Series[str] = pa.Field()
    race: Series[str] = pa.Field()

    @pa.check("column3")
    def column_3_check(cls, series: Series[str]) -> Series[bool]:
        """Check that column3 values have two elements after being split with '_'"""
        return series.str.split("_", expand=True).shape[1] == 2

Schema.validate(df)







