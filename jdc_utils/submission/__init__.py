from .core_measures import CoreMeasures,map_core_measures_to_sheepdog
from jdc_utils.utils import Gen3FileUpdate

def submit_package_to_jdc(
    package_path,
    commons_project,
    file_guid,
    sheepdog_id,
    credential_path="credentials.json",
    submission_type="upload"

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
    
    assert submission_type in ["update","create"]
    if submission_type=="update":
        assert file_guid and sheepdog_id,"Need both a file guid and sheep dog id for an update"
        output = gen3file_update.update()
    elif submission_type=="create"
        output = gen3file_update.upload_new_record()
    
    return output



