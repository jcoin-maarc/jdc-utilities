from .core_measures import CoreMeasures,map_core_measures_to_sheepdog
from jdc_utils.utils import Gen3FileUpdate

def submit_package_to_jdc(
    package_path,
    commons_project,
    file_guid,
    sheepdog_id,
    credential_path="credentials.json"

    ):
    """ 
    uploads a file (eg zipped core measure data package)
    and maps variables to current jdc data model

    TODO: allow updates to metadata
    TODO: support for first time uploads
    """

    gen3file_update = Gen3FileUpdate(
        #all hubs (and hence core measures) under program JCOIN
        commons_program="JCOIN", 
        commons_project=commons_project, 
        # commons_bucket:configured aws bucket for JDC
        commons_bucket="s3://jcoinprod-default-258867494168-upload", 
        file_guid=file_guid, 
        sheepdog_id=sheepdog_id,
        new_file_path=package_path,
        credentials_path=credential_path
    )
    return gen3file_update.update()



