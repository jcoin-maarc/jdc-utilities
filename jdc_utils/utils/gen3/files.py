# %%

import os
from pathlib import Path
import requests
import hashlib
import copy
import requests
import json

MAX_RETRIES = 4
def import_gen3():
    # NOTE: using the Gen3File object to get presigned url so no need for these
    # (previously used to call directly from commons API)
    # from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
    from gen3.file import Gen3File 
    from gen3.index import Gen3Index
    from gen3.submission import Gen3Submission
    from gen3.auth import Gen3Auth

    from gen3 import Gen3Error


    class Gen3SubmissionModified(Gen3Submission):
        """ 
        Modified Gen3Submission object that changes 
        methods with limitations. For example, submit_record
        doesn't support chunks which causes a request error with a large
        number of records submitted at once.

        Tried my best to make this easy to put into a future PR to gen3 SDK.

        """    
        def submit_record(self,program,project,json,chunk_size=30):
            """Submit record(s) to a project as json.
                Args:
                    program (str): The program to submit to.
                    project (str): The project to submit to.
                    json (object): The json defining the record(s) to submit. For multiple records, the json should be an array of records.
                    chunk_size (integer): The number of records of data to submit for each request to the API.                    
                Examples:
                    This submits records in groups of 30 to the CCLE project in the sandbox commons.
                    >>> Gen3Submission.submit_record("DCF", "CCLE", json,30)
            """
            print(
                "  Submitting {} records in batches of {}".format(
                    len(json), chunk_size
                )
            )

            n_batches = ceil(len(json) / chunk_size)
            for i in range(n_batches):
                records = json[
                    i * chunk_size : (i + 1) * chunk_size
                ]

                tries = 0
                while tries < MAX_RETRIES:
                    response = requests.put(
                        "{}/api/v0/submission/{}/{}".format(
                            self._endpoint, program, project
                        ),
                        json=json,
                    )
                    if response.status_code != 200:
                        tries += 1
                        sleep(5)
                    else:
                        print("Submission progress: {}/{}".format(i + 1, n_batches))
                        break
                if tries == MAX_RETRIES:
                    if "Entity is not unique" in response.text:
                        print(f"Couldn't submit the following records:\n {records}")
                    raise Exception(
                        "Unable to submit to Sheepdog: {}\n{}".format(
                            response.status_code, response.text
                        )
                    )

        def submit_records(self,program,project,json,chunk_size=30):
            self.submit_record(program,project,json,chunk_size)

        return Gen3File,Gen3Index,Gen3SubmissionWithChunks,Gen3Auth

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
    - running the `create` method will create an entirely new record. This is intended 
        to run only once for a given file.
    
    TODO: 1. metadata-service functionality 2. making updates to only metadata for a given set
     of records.
    TODO: replace SDK calls with calls directly to API?
    TODO: method to write metadata records to a file for easier reference 
    TODO: read function to read metadata records from said file (allows easier additions of metadata)
     --- make mapping or compataibility with fricitonless datapackage?
    TODO: make __repr__ or __str__ print the various records and properties

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
        new_file_path,
        file_guid=None,
        sheepdog_id=None,
        sheepdog_file_submitter_id=None,
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
        
        # read in file and get the new file info
        # get new file info TODO: expand this to remote paths
        with open(new_file_path, "rb") as f:
            self.new_file = f.read()
        self.new_md5sum = hashlib.md5(self.new_file).hexdigest()
        self.new_filesize = os.path.getsize(new_file_path)
        self.new_file_name = Path(new_file_path).name

        # get latest file info stored in indexd
        if file_guid:
            try:
                self.latest_index = latest_index = self.gen3index.get_latest_version(file_guid)
                self.latest_file = None  # this should always be None as it is already uploaded
                self.latest_md5sum = latest_index["hashes"]["md5"]
                self.latest_filesize = latest_index["size"]
                self.latest_guid = latest_index["did"]
                self.latest_file_name = latest_index["file_name"]
                self.same_md5sum_latest_index_and_new_file = self.latest_md5sum == self.new_md5sum
            except:
                print("Could not get latest indexd record")
                self.latest_index = None
        else:
            print("No file guid specified -- needed for updating records")
            self.latest_index = None
        # get sheepdog record
        if sheepdog_id or sheepdog_file_submitter_id:
            self.get_sheepdog_file_record(
                program, 
                project,
                sheepdog_id,
                sheepdog_file_submitter_id)
            if self.latest_sheepdog_record:
                self.latest_sheepdog_guid = sheepdog_guid = self.latest_sheepdog_record['object_id']
                self.latest_sheepdog_md5sum = sheepdog_md5sum = self.latest_sheepdog_record['md5sum']   
            else:
                print("No sheepdog record with the given sheepdog id or submitter id")
        else:
            print("No sheepdog id/submitter id specified")
        

        # checks to make sure three services/file metadata locations are in sync

        if self.latest_sheepdog_record and self.latest_index:
            self.same_guid_latest_sheepdog_and_latest_index = sheepdog_guid == self.latest_guid
            self.same_md5sum_latest_sheepdog_and_new_file = sheepdog_md5sum == self.latest_md5sum
            self.same_md5sum_latest_index_and_sheepdog = self.latest_md5sum == sheepdog_md5sum
            if self.same_md5sum_latest_index_and_sheepdog and self.same_guid_latest_sheepdog_and_latest_index:
                print("The latest sheepdog and indexd have same file:GOOD")
            else:
                print("Be careful -- sheepdog and indexd have different files. Investigate further before updating")

        if self.latest_index:
            if self.same_md5sum_latest_index_and_new_file:
                print("The new file is the same as latest indexd file. No need to update (unless updating metadata).")
        else:
            print("No indexd guid specified or record found-- need to upload a new record/file? use the .create method")
    
    def update(self):
        """ 
        Updates an existing file record with a new version of file attached to 
        a generated unique id (GUID;object id)
        
        
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
                self.update_sheepdog_file_record()
                #TODO: embed exception into each of the above fxns for specificity
                 # delete file locations in upload new version; reload latest sheepdog rec in 2nd fxn
            except Exception as e: 
                print("File upload failed see error message below:")
                print()
                print(e)
                self.gen3files.delete_file_locations(self.new_guid)

        return self
    
    def create(
        self,
        file_node_submitter_id=None,
        cmc_node_submitter_id=None,
        data_category=None,
        data_format=None,
        data_type=None,
        other_file_node_metadata=None,
        other_cmc_node_metadata=None):

        """ Create a new set of records for
        given file in gen3 microservices

        If no submitter ids specified, will default to file name stem.
        Metadata is optional as well but highly recommended (to make the file findable)

        """
        is_no_record = not self.latest_index and not self.latest_sheepdog_record
        assert is_no_record,"There is a sheepdog and indexd record that already exists"
        try:
            self.upload_new_file()
            self.create_sheepdog_file_record(
                file_node_submitter_id=file_node_submitter_id,
                cmc_node_submitter_id=cmc_node_submitter_id,
                data_category=data_category,
                data_format=data_format,
                data_type=data_type,
                other_file_node_metadata=other_file_node_metadata,
                other_cmc_node_metadata=other_cmc_node_metadata
            )
        except requests.exceptions.HTTPError as e:
            print("File upload failed see error message below:")
            print()
            print(json.dumps(e.response.json(),indent=4))
            self.gen3files.delete_file_locations(self.new_guid)

    def upload_new_file(self):
        """
        Upload and map a new file without existing file in storage location/indexd record
        
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

    def get_sheepdog_file_record(self,program,project,
        sheepdog_id=None,sheepdog_file_submitter_id=None):
        """ 
        get a sheepdog file node record based on either a uuid 
        or submitter id

        TODO: instead of exporting entire node, directly query
        (functionality not in SDK so use API)
        """
        try:
            if sheepdog_id:
                # get latest sheepdog record
                self.latest_sheepdog_record = self.gen3sheepdog.export_record(
                    program, project, sheepdog_id, "json"
                )[0]
            elif sheepdog_file_submitter_id:
                node_records = self.gen3sheepdog.export_node(
                    program=program,
                    project=project,
                    node_type="reference_file",
                    fileformat="json")
                
                node_record = [
                    record for record in node_records["data"] 
                    if record["submitter_id"]==sheepdog_file_submitter_id
                ]
                
                if node_record:
                    assert len(node_record)==1
                    self.latest_sheepdog_record = node_record[0]
                else:
                    self.latest_sheepdog_record = None
                    
        except request.exception.HTTPError:
            print("Could not get latest sheepdog record")
            self.latest_sheepdog_record = None
            return self

    # map to sheepdog
    def create_sheepdog_file_record(self,
        file_node_submitter_id=None,
        cmc_node_submitter_id=None,
        data_category=None,
        data_format=None,
        data_type=None,
        other_file_node_metadata=None,
        other_cmc_node_metadata=None):
        """ 
        Creates a new sheepdog 
        record from a new indexd record

        If the provided core_metadata collection node submitter id (`cmc_node_submitter_id`)
        exists (but not the reference file submitter id), will add the reference file record
        to the existing reference file node.

        If both nodes' submitter ids exist, will simply update the current records. 

        If no submitter ids specified, will use the file name stem. 
        """ 

        program = self.commons_program
        project = self.commons_project

        assert hasattr(self,'new_index'),"Need to create a new indexd record with new file. Use "

        if not file_node_submitter_id:
            file_node_submitter_id = Path(self.new_file_name).stem
        
        if not cmc_node_submitter_id:
            cmc_node_submitter_id = file_node_submitter_id
        
        if not other_file_node_metadata:
            other_file_node_metadata = {}
        
        if not other_cmc_node_metadata:
            other_cmc_node_metadata = {}

        self.new_sheepdog_record = {
            'project_id': f'{program}-{project}',
            'submitter_id': file_node_submitter_id,
            'file_name': self.new_file_name,
            'file_size':self.new_filesize,
            'md5sum':self.new_md5sum,
            "object_id": self.new_guid,
            
            **other_file_node_metadata,
            'core_metadata_collections': [
                {
                "project_id":f"{program}-{project}",
                'submitter_id': cmc_node_submitter_id,
                "type":"core_metadata_collection",
                "projects":[{"code":project}],
                **other_cmc_node_metadata
                }
            ],
            'type': 'reference_file'}

        if data_category:
            self.new_sheepdog_record["data_category"] = data_category
        
        if data_type:
            self.new_sheepdog_record['data_type'] = data_type 

        if data_format:
            self.new_sheepdog_record['data_format'] = data_format


        # upload parent node (core metadata collection) and then child node (reference_file) with 
         # link to parent node
        self.gen3sheepdog.submit_record(
                    program, project, json=self.new_sheepdog_record['core_metadata_collections']
                )
        self.new_sheepdog_submit_output = self.gen3sheepdog.submit_record(
            program, project, json=self.new_sheepdog_record
        )

        return self

    def update_sheepdog_file_record(self):
        """
        Updates the latest (existing) sheepdog record with the new file
        properties (ie GUID, file name, md5sum)
        """

        program = self.commons_program
        project = self.commons_project
        assert self.latest_sheepdog_record,"No sheepdog record -- check to make sure you entered the correct submitter_id or sheepdog_id (ie node_id)"
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


