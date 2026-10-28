-- @materialize: true
-- Materialized: stored once as a DuckDB table (rebuilt when the data is refreshed, see `make rill-refresh`).
-- Kubernetes Engine net cost per day, project, namespace and workload (DuckDB mode only), for the
-- "Kubernetes cost by workload" table on the "GCP Cost by Service and App" page.
-- The workload comes from GKE cost allocation's k8s-workload-name label (gcp_labels_per_row). Cost without
-- it (before cost allocation was turned on, or cluster fees) is kept as "(no workload label)" so the
-- totals still match the Kubernetes Engine cost on the other tables.
SELECT
  c.date,
  c.project_id AS project__id,
  COALESCE(l.k8s_namespace, '(no namespace label)') AS k8s_namespace,
  COALESCE(l.k8s_workload, '(no workload label)') AS k8s_workload,
  SUM(c.net_cost) AS net_cost
FROM {{ ref "gcp_costs" }} c
LEFT JOIN {{ ref "gcp_labels_per_row" }} l ON l._dlt_parent_id = c._dlt_id
WHERE c.service_name = 'Kubernetes Engine'
GROUP BY ALL
