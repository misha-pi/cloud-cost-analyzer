-- Rules that assign each GCP billing row to an app (used by the "GCP Cost by Service and App" page).
-- Edit this list to add an app or change a rule; the first matching rule (lowest priority) wins and
-- rows matching no rule go to "other".
--
-- match_on:
--   project  pattern is matched against the project id
--   service  pattern is matched against the service name (e.g. "Cloud DNS")
--   name     pattern is matched against the k8s-namespace label, then the app label, then the resource name
-- Patterns are lowercase SQL LIKE patterns.
--
-- Same rules as billing-helper (gitlab pi-network/pi-app-engine/billing-helper), which built the monthly sheet:
-- the main app project is all "main app", some services go to a fixed app, everything else is matched by
-- name (GKE namespace, Cloud SQL instance, bucket). "_support" rows are spread over the apps by cost share
-- in gcp_app_costs.
SELECT * FROM (VALUES
  (10, 'project', 'main-app-production%', 'main app'),

  (20, 'service', 'support', '_support'),
  (21, 'service', 'cloud dns', 'miscellaneous'),
  (22, 'service', 'cloud logging', 'miscellaneous'),
  (23, 'service', 'cloud monitoring', 'miscellaneous'),
  (24, 'service', 'certificate manager', 'miscellaneous'),
  (25, 'service', 'cloud memorystore%', 'notify'),
  (26, 'service', 'cloud vision api', 'kyc'),
  (27, 'service', '%translat%', 'fireside'),
  (28, 'service', 'web risk%', 'fireside'),

  -- Same order as billing-helper's PI_APPS: a name containing several app names gets the first one
  (100, 'name', '%analytics%', 'analytics'),
  (101, 'name', '%auth%', 'auth'),
  (102, 'name', '%appstudio%', 'appstudio'),
  (103, 'name', '%blockexplorer%', 'blockexplorer'),
  (104, 'name', '%brainstorm%', 'brainstorm'),
  (105, 'name', '%chat%', 'chat'),
  (106, 'name', '%devportal%', 'devportal'),
  (107, 'name', '%domains%', 'domains'),
  (108, 'name', '%e2e%', 'e2e'),
  (109, 'name', '%ecosystem%', 'ecosystem'),
  (110, 'name', '%fireside%', 'fireside'),
  (111, 'name', '%fruity%', 'fruity'),
  (112, 'name', '%kyc%', 'kyc'),
  (113, 'name', '%launchpad%', 'launchpad'),
  (114, 'name', '%mapofpi%', 'mapofpi'),
  (115, 'name', '%matchpi%', 'matchpi'),
  (116, 'name', '%notify%', 'notify'),
  (117, 'name', '%piano%', 'piano'),
  (118, 'name', '%pinet%', 'pinet'),
  (119, 'name', '%piverify%', 'piverify'),
  (120, 'name', '%profiles%', 'profiles'),
  (121, 'name', '%sliceofpi%', 'sliceofpi'),
  (122, 'name', '%solohost%', 'solohost'),
  (123, 'name', '%supportbot%', 'supportbot'),
  (124, 'name', '%sync%', 'sync'),
  (125, 'name', '%translate%', 'translate')
) AS rules(priority, match_on, pattern, app)
