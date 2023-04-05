# %%

import os
from pathlib import Path
import requests
import hashlib
import copy

def import_gen3():
    # NOTE: using the Gen3File object to get presigned url so no need for these
    # (previously used to call directly from commons API)
    # from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
    from gen3.file import Gen3File 
    from gen3.index import Gen3Index
    from gen3.submission import Gen3Submission
    from gen3.auth import Gen3Auth
    return Gen3File,Gen3Index,Gen3Submission,Gen3Auth

class Gen3FileUpdate:
    """
    automates new file uploads and syncing
    between file object storage, indexd, and sheepdog.
    
    - initiating this object WONT update anything.
    It WILL get all information related
    to the to-be-uploaded new file (eg local file)
    and the latest metadata associated with that file from 
    the gen3 commons.
    - running the `update` method will update and sync all microservices
        referencing the file.

    Example
    --------
    gen3file_update = Gen3FileUpdate(
            commons_program="FAKE", 
            commons_project="TEST", 
            commons_bucket="s3://not-a-real-bucket-upload", 
            file_guid="dg.XXXX/2not2-2a2-2valid2-2guid2-2justfake2", 
            sheepdog_id="fake22-id22-for2-demo-helloworld22",
            new_file_path="your/local/path",
            credentials_path="credentials.json"
        )
        self.gen3 = gen3file_update.update()
    """

    def __init__(
        self,
        commons_program,
        commons_project,
        commons_bucket,
        file_guid,
        sheepdog_id,
        new_file_path,
        credentials_path="credentials.json",
    ):

        try:
            Gen3File,Gen3Index,Gen3Submission,Gen3Auth = import_gen3()
        except ImportError as e:
            raise Exception("gen3 package failed to import. Try installing with `pip install gen3`") from e

        # instantiate gen3 SDK microservice objects 
        self.gen3auth= Gen3Auth(refresh_file=credentials_path)
        self.gen3index = Gen3Index(self.gen3auth)
        self.gen3sheepdog = Gen3Submission(self.gen3auth)
        self.gen3files = Gen3File(self.gen3auth)

        self.commons_bucket = commons_bucket
        self.commons_url = self.gen3auth.endpoint
        self.commons_program = program = commons_program
        self.commons_project = project = commons_project

        default_authz = "/programs/{}/projects/{}".format(program, project)
        self.authz = [default_authz]

        # get latest file info stored in index
        if file_guid:
            self.latest_index = latest_index = self.gen3index.get_latest_version(file_guid)
            self.latest_file = None  # this should always be None as it is already uploaded
            self.latest_md5sum = latest_index["hashes"]["md5"]
            self.latest_filesize = latest_index["size"]
            self.latest_guid = latest_index["did"]
            self.latest_file_name = latest_index["file_name"]
        if sheepdog_id:
            # get latest sheepdog record
            self.latest_sheepdog_record = self.gen3sheepdog.export_record(
                program, project, sheepdog_id, "json"
            )[0]
            # get new file info TODO: expand this to remote paths
        with open(new_file_path, "rb") as f:
            self.new_file = f.read()
        self.new_md5sum = hashlib.md5(self.new_file).hexdigest()
        self.new_filesize = os.path.getsize(new_file_path)
        self.new_file_name = Path(new_file_path).name

        # checks to make sure three services/file metadata locations are in sync
        if sheepdog_id:
            self.latest_sheepdog_guid = sheepdog_guid = self.latest_sheepdog_record['object_id']
            self.latest_sheepdog_md5sum = sheepdog_md5sum = self.latest_sheepdog_record['md5sum']

        if sheepdog_id and file_guid:
            self.same_guid_latest_sheepdog_and_latest_index = sheepdog_guid == self.latest_guid
            self.same_md5sum_latest_sheepdog_and_new_file = sheepdog_md5sum == self.latest_md5sum
            self.same_md5sum_latest_index_and_sheepdog = self.latest_md5sum == sheepdog_md5sum
            if self.same_md5sum_latest_index_and_sheepdog and self.same_guid_latest_sheepdog_and_latest_index:
                print("The latest sheepdog and indexd have same file:GOOD")
            else:
                print("Be careful -- sheepdog and indexd have different files. Investigate further before updating")

        if file_guid:
            self.same_md5sum_latest_index_and_new_file = self.latest_md5sum == self.new_md5sum


            if self.same_md5sum_latest_index_and_new_file:
                print("The new file is the same as latest indexd file. No need to update (unless updating metadata).")
        

        if not sheepdog_id:
            print("No sheepdog id -- need to create a new sheepdog record that points to a file?")
        
        if not file_guid:
            print("No indexd guid specified -- need to upload a new record/file?")
    
    def update(self):
        """ 
        given the file guid and sheepdog id, updates and syncs all 
        microservices (file storage, indexd, and sheepdog) with latest version of
        file. 
        TODO: handling syncing/adding file manifests and metadata to metadata-service.
        """ 
        if self.same_md5sum_latest_index_and_new_file:
            print("File is the same as latest file in indexd so not updating")
        else:
            try:
                self.upload_new_version()
                self.update_sheepdog_file_node()
            except Exception as e:
                print("File upload failed see error message below:")
                print()
                print(e)
                self.gen3files.delete_file_locations(self.new_guid)

        return self

    def upload_new_record(self):
        """
        while upload_new_version assumes there is a current file assigned to a guid with
        an associated baseid, if one wants to upload an unmapped file (eg new file),
        this function uploads to initate a new reocrd for this file's storage and indexd.
        """ 

        program = self.commons_program
        project = self.commons_project
        # update new record more index metadata
        # TODO: add version string

        self.new_guid = requests.get(
            f"{self.commons_url}/index/guid/mint?count=1"
        ).json()["guids"][0]

        self.gen3index.create_record(
            hashes={"md5": self.new_md5sum},
            file_name=self.new_file_name,
            size=self.new_filesize,
            did=self.new_guid,
            authz=self.authz
        
        )
        self.upload_to_file_storage()

    def upload_new_version(self):
        """
        With a new guid generated, uploads the new file to the commons data source location (eg AWS Bucket)
        and adds a new indexd record for the new file
        """

        program = self.commons_program
        project = self.commons_project
        # update new record more index metadata
        # TODO: add version string

        self.new_guid = requests.get(
            f"{self.commons_url}/index/guid/mint?count=1"
        ).json()["guids"][0]

        self.new_index = self.gen3index.create_new_version(
            guid=self.latest_guid,
            hashes={"md5": self.new_md5sum},
            file_name=self.new_file_name,
            size=self.new_filesize,
            did=self.new_guid,
            authz=self.authz,
        )
        assert self.new_guid == self.new_index["did"]
        self.upload_to_file_storage()
        return self

    def upload_to_file_storage(self):
        # get presigned url to upload
        new_file_presign = self.gen3files.upload_file_to_guid(
            self.new_guid, self.new_file_name, expires_in=3600
        )
        headers = {
            "Content-Type": "application/binary",
        }

        # upload file to preseign url
        upload = requests.put(
            new_file_presign["url"], data=self.new_file, headers=headers
        )
        upload.raise_for_status()
        self.new_file_url = (
            f"{self.commons_bucket}/{self.new_guid}/{self.new_file_name}"
        )
        self.new_index = self.gen3index.update_record(
            self.new_guid, 
            urls=[self.new_file_url])
        return self

    # map to sheepdog
    def update_sheepdog_file_node(self):
        """
        Updates the latest sheepdog record with the new file
        properties (ie GUID, file name, md5sum)
        """

        program = self.commons_program
        project = self.commons_project

        self.new_sheepdog_record = copy.deepcopy(self.latest_sheepdog_record)
        self.new_sheepdog_record.update(
            {
                "md5sum": self.new_md5sum,
                "file_size": self.new_filesize,
                "file_name": self.new_file_name,
                "object_id": self.new_guid,
            }
        )
        self.new_sheepdog_submit_output = self.gen3sheepdog.submit_record(
            program, project, json=self.new_sheepdog_record
        )

        return self


