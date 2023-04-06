from .core_measures import CoreMeasures,map_core_measures_to_sheepdog
from jdc_utils.utils import Gen3FileUpdate

def submit_package_to_jdc(
    package_path,
    commons_project,
    file_guid=None,
    sheepdog_id=None,
    sheepdog_file_submitter_id=None,
    sheepdog_data_category=None,
    sheepdog_data_format=None,
    sheepdog_data_type=None,
    credential_path="credentials.json",
    submission_type="update"

    ):
    """ 
    uploads a file (eg zipped core measure data package)
    and maps variables to current jdc data model (either new or existing files)

    TODO: allow updates just to metadata

    submission_type of "create" is for first time file uploads (need to add accompanying metadata -- 
     see `Gen3FileUpdate.create_sheepdog_file_record` docs for details)

    submission_type of "update" is for subsequent updates to version of file

    sheepdog_submitter_id needed for new uploads and can be used for updates as well
    sheepdog_id only for new records currently
    

    """

    gen3file_update = Gen3FileUpdate(
        #all hubs (and hence core measures) under program JCOIN
        commons_program="JCOIN", 
        commons_project=commons_project, 
        # commons_bucket:configured aws bucket for JDC
        commons_bucket="s3://jcoinprod-default-258867494168-upload", 
        file_guid=file_guid, 
        sheepdog_id=sheepdog_id,
        sheepdog_file_submitter_id=sheepdog_file_submitter_id,
        new_file_path=package_path,
        credentials_path=credential_path
    )
    
    assert submission_type in ["update","create"]
    if submission_type=="update":
        has_sheepdog = sheepdog_id or sheepdog_submitter_id
        assert file_guid and has_sheepdog,"Need both a file guid and sheep dog id for an update"
        output = gen3file_update.update()
    elif submission_type=="create":
        has_sheepdog_metadata = (
            sheepdog_file_submitter_id and
            sheepdog_data_category and 
            sheepdog_data_format and 
            sheepdog_data_type
          )
        assert has_sheepdog_metadata, "Need to add sheepdog file metadata"
        output = gen3file_update.create(
            data_category=sheepdog_data_category,
            data_format=sheepdog_data_format,
            data_type=sheepdog_data_type,
        )
    
    return output



