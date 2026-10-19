# MongoDB Atlas billing: schema

Our MongoDB Atlas invoices are synced into a MongoDB database, `atlas_billing`.
The pipeline (`pipelines/mongodb_atlas_billing_pipeline.py`) reads it read-only and loads two of its
three collections for the cost dashboards (PI-18056).

No billing data is committed. `scripts/generate_atlas_fixtures.py` generates documents with the
production field names, key order and BSON types below, but every value (org, project and cluster
names, IDs, quantities, prices, costs) is made up. It writes one canonical Extended JSON document per
line, the format of `mongoexport --jsonFormat=canonical`. The tests generate them into a temporary
folder, and `tests/test_atlas_billing_pipeline.py` checks that they keep exactly this schema.

## Which collections the dashboards use

| Collection | Used | Why |
|---|---|---|
| `lineItems` | **Yes, main source** | One document per day, project, cluster and SKU. The only collection with cost per cluster per day. For every closed month its costs add up exactly to the invoice subtotal. |
| `invoices` | Header only | Monthly totals (subtotal, credits, billed, status) to check that the line items are complete. The embedded line items duplicate `lineItems` and are capped (see below), so they are not loaded. |
| `orgInfo` | No | The single org's id and name, which every line item already carries in `org`. |

## `lineItems`

| Field | BSON type | Notes |
|---|---|---|
| `_id` | ObjectId | |
| `category` | string | Instances, Storage, Backup, Data Transfer, Data Lake, Private Endpoint, Charts, Support, Other |
| `cluster` | string | Cluster name. **Not unique across projects** (the same name in Production and Staging). `--n/a--` for org-level lines (Support, Charts, Data Lake, ...) |
| `cost` | Decimal128 | Dollars, rounded to the cent, 13 to 16 decimal places. No negative (credit) lines |
| `date` | Date | Usage day, midnight UTC |
| `instance` | string | Cluster tier for Instances lines (`M10`, `M60_LOW_CPU`, `M10_PAUSED`, ...), otherwise `non-instance` |
| `invoiceId` | string | `invoices.id` of the month's invoice |
| `org` | object | `{id, name}`; one org |
| `project` | object | `{id, name}`, e.g. "Orders - Production". **Empty `{}` for Support lines** (and the odd other line) |
| `provider` | string | `GCP`, `AWS` (Data Lake) or `n/a` (Support, Charts) |
| `quantity` | Double or Int32 | Int32 when whole |
| `sku` | string | e.g. `ATLAS_GCP_INSTANCE_M10`, `ATLAS_GCP_DATA_TRANSFER_INTERNET`, `ATLAS_SUPPORT` |
| `tags` | object | Only on a few lines, always `{}` so far. Probably Atlas resource tags; kept as JSON |
| `unit` | string | server hours, GB, GB hours, GB days, TB, days, hours |
| `unitPriceDollars` | Decimal128 | |

## `invoices`

One document per invoice, with header fields in **cents** (Int32): `amountBilledCents`,
`amountPaidCents`, `creditsCents`, `salesTaxCents`, `startingBalanceCents`, `subtotalCents`; dates as
ISO strings (`startDate`, `endDate`, `created`, `updated`); `id`, `orgId`, `statusName`
(e.g. PAID, PENDING), `payments[]`, `refunds[]`, `links[]`, `linkedInvoiceIds[]`.

- **Embedded line items are capped at 10,000 per document.** A bigger invoice is stored as several
  documents with the same `id` and header and `segment` 0, 1, ... The models keep one row per `id`.
- **The open month (PENDING) has a near-zero `subtotalCents`** until it closes; use the line items for
  the month to date.
- **Credits:** `amountBilledCents = subtotalCents + salesTaxCents`. When credits cover a bill,
  `creditsCents` equals `amountBilledCents` and `amountPaidCents` is 0, so the dashboards chart the
  line item cost (equal to the subtotal), not the amount paid.
- Embedded line items (`lineItems[]`) have `cloudProvider`, `clusterName` (missing on some backup and
  org-level lines), `groupId`/`groupName` (the project), `startDate`/`endDate`, `quantity`, `sku`,
  `totalPriceCents`, `unit`, `unitPriceDollars`, `created`. The fixtures' embedded copies of Support
  lines (no `groupId`/`groupName`, `cloudProvider` "NONE") are a guess: no production sample had one.
  The pipeline does not read the embedded line items.

## Checks against production

- The sum of `lineItems.cost` per `invoiceId` equals `subtotalCents` for every closed invoice.
- An invoice can have no line items in `lineItems` if the sync missed it; the invoice table on the
  Atlas page shows that as a non-zero "Subtotal minus line items".

## Generated data

```bash
make test-atlas        # generates Sep - Oct 2026 into a temporary folder and runs the tests
make atlas-demo-data   # 1 January last year to today, loaded into viz_rill/data/atlas_billing (gitignored)
uv run python scripts/generate_atlas_fixtures.py --out /tmp/atlas_fixtures   # just the JSON files
```

`make demo` (and the Docker demo) run `make atlas-demo-data`'s script, `scripts/atlas_demo_data.sh`, after
copying the bundled GCP/AWS demo data.
