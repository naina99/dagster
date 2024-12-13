#Dagster job usbmit , sttus poll through web and UI
from dagster import op
from dagster_graphql import DagsterGraphQLClient
from dagster import DagsterRunStatus
import requests
import time
query1 = """query JobsQuery(
  $repositoryLocationName:  String!,
  $repositoryName: String!
) {
  repositoryOrError(
    repositorySelector: {
      repositoryLocationName: $repositoryLocationName,
      repositoryName: $repositoryName
    }
  ) {
    ... on Repository {
      jobs {
        name
      }
    }
  }
}"""

variables  = {"repositoryLocationName": "with_pyspark", "repositoryName": "__repository__"}

url = 'http://localhost:3000/graphql'

r1 = requests.post(url, json={'query': query1, 'variables': variables})
print(r1.text)

dagster_client = DagsterGraphQLClient("localhost", port_number=3000)

# Trigger the job and get the run ID
response = dagster_client.submit_job_execution(job_name="__ASSET_JOB")
print(response)
run_id = response

# Poll the status of the job until it completes
while True:
    status_response = dagster_client.get_run_status(run_id)
    print(status_response)
    status = status_response

    if status in [DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE]:
        break

    # Implement some delay between polls to avoid overwhelming the server
    time.sleep(1)
        
