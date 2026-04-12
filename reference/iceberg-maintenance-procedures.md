# Iceberg 1.10.1 Maintenance Procedures — Reference

Captured 2026-04-12 from
<https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.10.1/docs/docs/spark-procedures.md>
to inform the `spark/analysis/maintenance.py` design and the
`/api/maintenance/*` endpoints in `query-api`.

## `rewrite_data_files`

Compacts small data files into larger ones to reduce metadata overhead and
file-open cost. Combine with `expire_snapshots` afterwards to actually free
storage — `rewrite_data_files` only marks the old files for removal at the
next snapshot expiry.

### Arguments

| Name | Required | Type | Notes |
|---|---|---|---|
| `table` | yes | string | Fully-qualified table name |
| `strategy` | no | string | `binpack` (default) or `sort` |
| `sort_order` | no | string | For sort strategy. Supports `zorder(c1,c2)` |
| `options` | no | `map<string, string>` | Tuning knobs (see below) |
| `where` | no | string | Predicate restricting which files to rewrite |

### Critical options for our use case

| Name | Default | Why we care |
|---|---|---|
| `min-input-files` | **5** | A table with <5 small files won't be compacted at all unless we override. Mart tables routinely sit at 2–4 files; pass `'2'`. |
| `target-file-size-bytes` | 512 MB | Fine for big tables; harmless for small ones (everything packs into one file). |
| `partial-progress.enabled` | false | Set `'true'` for the big raw/staging tables so a single file-group failure doesn't roll back the whole rewrite. |
| `max-concurrent-file-group-rewrites` | 5 | Default is fine; query-api isn't bandwidth-bound. |
| `min-file-size-bytes` | 75% × target | Files below this are eligible for rewriting regardless of `min-input-files`. |
| `rewrite-all` | false | Force rewrite of all files; use only for emergency cleanups. |
| `delete-file-threshold` | INT_MAX | Min number of position deletes against a data file before it's eligible. Not relevant — we don't have position deletes. |
| `remove-dangling-deletes` | false | We don't have deletes to dangle; leave off. |

### Output columns (read these from the result row)

| Name | Type |
|---|---|
| `rewritten_data_files_count` | int |
| `added_data_files_count` | int |
| `rewritten_bytes_count` | long |
| `failed_data_files_count` | int |
| `removed_delete_files_count` | int |

### Example invocation we're using

```sql
CALL atmosphere.system.rewrite_data_files(
    table => 'atmosphere.mart.mart_trending_hashtags',
    options => map('min-input-files', '2', 'partial-progress.enabled', 'true')
)
```

## `expire_snapshots`

Removes old snapshots and any data files only those snapshots reference.
Will never delete files still required by a non-expired snapshot. Run this
*after* `rewrite_data_files` so the rewritten-away files actually get freed.

### Arguments

| Name | Required | Type | Notes |
|---|---|---|---|
| `table` | yes | string | |
| `older_than` | no | timestamp | Default: 5 days ago. We pass 1 day. |
| `retain_last` | no | int | Default 1. Always retains at least this many ancestors. |
| `max_concurrent_deletes` | no | int | Threadpool for delete actions. Default: serial. |
| `stream_results` | no | boolean | Recommended `true` for big drops to avoid driver OOM. |
| `snapshot_ids` | no | array<long> | Explicit list. Mutually exclusive with `older_than`. |
| `clean_expired_metadata` | no | boolean | Drop unreferenced partition specs/schemas. |

### Output columns

| Name | Type |
|---|---|
| `deleted_data_files_count` | long |
| `deleted_position_delete_files_count` | long |
| `deleted_equality_delete_files_count` | long |
| `deleted_manifest_files_count` | long |
| `deleted_manifest_lists_count` | long |
| `deleted_statistics_files_count` | long |

### Example invocation we're using

```sql
CALL atmosphere.system.expire_snapshots(
    table => 'atmosphere.mart.mart_trending_hashtags',
    older_than => TIMESTAMP '2026-04-11 00:00:00',
    stream_results => true
)
```

## Out of scope

- `remove_orphan_files` — useful but slow (full directory listing) and not
  part of the approved threshold-driven cycle. Run manually if storage drift
  is suspected.
- `rewrite_manifests` — only helps when manifests get fragmented; the
  rewrite_data_files action already coalesces manifests transitively in
  most cases.
- `rewrite_position_delete_files` — we don't use position deletes.

## Why these specific procedures back the threshold trigger

The approved threshold is `file_count > 500 OR avg_file_kb < 30`. Both
conditions are symptoms of "too many small files". `rewrite_data_files` is
the direct fix; `expire_snapshots` then converts the marked-for-removal
files into actual storage savings. Without `expire_snapshots`, snapshot
history would keep growing forever and the metadata size would dominate the
data size on hot tables.
