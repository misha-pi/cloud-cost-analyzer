from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

c = bigquery.Client(project="billing-dev", credentials=AnonymousCredentials(),
                    client_options=ClientOptions(api_endpoint="http://localhost:9050"))
T = "`billing-dev.billing.gcp_billing_export_v1_FAKE00_000000_000000`"
q = lambda sql: print(c.query(sql).result(timeout=30).to_dataframe(create_bqstorage_client=False))

q(f"SELECT project.id, ROUND(SUM(cost),2) cost FROM {T} GROUP BY 1 ORDER BY 2 DESC")
q(f"SELECT DATE(usage_start_time) d, ROUND(SUM(cost),2) cost FROM {T} WHERE project.id='acme-prod-data' GROUP BY d ORDER BY cost DESC LIMIT 3")
q(f"SELECT c.type, ROUND(SUM(c.amount),2) total FROM {T}, UNNEST(credits) c GROUP BY 1")
q(f"SELECT pl.value team, ROUND(SUM(cost),2) cost FROM {T}, UNNEST(project.labels) pl WHERE pl.key='team' GROUP BY 1")
q(f"SELECT ROUND(SUM(cost),2) unlabeled FROM {T} WHERE ARRAY_LENGTH(labels)=0")