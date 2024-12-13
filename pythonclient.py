from dagster_graphql import DagsterGraphQLClient

client = DagsterGraphQLClient("localhost", port_number=3000)
from dagster_graphql import DagsterGraphQLClientError

from dagster import DagsterRunStatus
RUN_ID = "48884444-2308-4634-903f-0082af103ef7"
try:
    status: DagsterRunStatus = client.get_run_status(RUN_ID)
    print("Status")
    print(status)
    if status == DagsterRunStatus.SUCCESS:
        print("Success")
    else:
        print(status)
except DagsterGraphQLClientError as exc:
    print(exc)
    raise exc

