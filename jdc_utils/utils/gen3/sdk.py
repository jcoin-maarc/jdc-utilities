""" contains object imports and gen3 sdk object modifications 
to be imported into various objects/functions in rest of module

Goal is to develop functions that can be used for optional
dependencies while maintaining structure of SDK in case 
these would be useful to merge into SDK codebase.
""" 

def import_gen3():
    # NOTE: using the Gen3File object to get presigned url so no need for these
    # (previously used to call directly from commons API)
    # from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
    from gen3.file import Gen3File 
    from gen3.index import Gen3Index
    from gen3.auth import Gen3Auth

    Gen3Submission = import_modified_submission()
    
    return Gen3File,Gen3Index,Gen3Submission,Gen3Auth
    
def import_modified_submission():
    from math import ceil
    from time import sleep
    import requests
    from collections.abc import MutableMapping
    from gen3.submission import Gen3Submission

    MAX_RETRIES = 2
    class Gen3SubmissionModified(Gen3Submission):
        """ 
        Modified Gen3Submission object that changes 
        methods with limitations. For example, submit_record
        doesn't support chunks which causes a request error with a large
        number of records submitted at once.

        Tried my best to make this easy to put into a future PR to gen3 SDK.

        """ 

        def submit_record(self,program,project,json_records,chunk_size=100):
            """Submit record(s) to a project as an array of json records.
                Args:
                    program (str): The program to submit to.
                    project (str): The project to submit to.
                    json_record (object): The json_record defining the record(s) to submit. For multiple records, the json_record should be an array of records.
                    chunk_size (integer): The number of records of data to submit for each request to the API.                    
                Examples:
                    This submits records in groups of 30 to the CCLE project in the sandbox commons.
                    >>> Gen3Submission.submit_record("DCF", "CCLE", json_record,30)
            """
            print(
                "  Submitting {} records in batches of {}".format(
                    len(json_records), chunk_size
                )
            )

            # allow one record to be batched by turning into a json array
            if isinstance(json_records,MutableMapping):
                json_records = [json_records]

            n_batches = ceil(len(json_records) / chunk_size)
            for i in range(n_batches):
                json_records_batch = json_records[
                    i * chunk_size : (i + 1) * chunk_size
                ]

                tries = 0
                while tries < MAX_RETRIES:
                    response = requests.put(
                        "{}/api/v0/submission/{}/{}".format(
                            self._endpoint, program, project
                        ),
                        json=json_records_batch,
                        auth=self._auth_provider
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

        def submit_records(self,program,project,json_records,chunk_size=30):
            """ 
            wrapper convenience function for submitting multiple records
            """ 
            self.submit_record(program,project,json_records,chunk_size)

    return Gen3SubmissionModified
