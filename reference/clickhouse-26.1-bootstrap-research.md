# ClickHouse 26.1 bootstrap research notes

Research pass before CH-01/02/03 implementation. Complements [clickhouse-iceberg-research.md](clickhouse-iceberg-research.md) and [clickhouse-migration-plan.md](clickhouse-migration-plan.md).

## Image: `clickhouse/clickhouse-server:26.1-alpine`

From upstream [`docker/server/Dockerfile.alpine`](https://github.com/ClickHouse/ClickHouse/blob/master/docker/server/Dockerfile.alpine):

- Installs `clickhouse-client`, `clickhouse-server`, `clickhouse-common-static` from the ClickHouse repo.
- Adds `bash` and `tzdata` via `apk`.
- `wget` is available (BusyBox wget ships with Alpine base; Dockerfile's build step also uses `wget`).
- `curl` is **not** present.
- `tini` is not present.

Implication for CH-01 healthcheck: `wget -qO- http://localhost:8123/ping` is fine. No need to fall back to the full Ubuntu image or custom Dockerfile for this reason.

## DataLakeCatalog → Polaris DDL

Official [Polaris catalog docs](https://clickhouse.com/docs/use-cases/data-lake/polaris-catalog):

```sql
CREATE DATABASE polaris_catalog
ENGINE = DataLakeCatalog('https://<uri>/api/catalog/v1')
SETTINGS
    catalog_type = 'rest',
    catalog_credential = '<client-id>:<client-secret>',
    warehouse = 'snowflake',
    auth_scope = 'PRINCIPAL_ROLE:ALL',
    oauth_server_uri = 'https://<uri>/api/catalog/v1/oauth/tokens',
    storage_endpoint = '<storage_endpoint>'
```

Notes:
- **Version floor:** 26.1+.
- **Experimental flag:** server session must have `allow_experimental_database_unity_catalog = 1`. The plan pins this profile-wide via `docker/clickhouse/config.d/datalake.xml` so Grafana sessions pick it up without a `SET`.
- **`warehouse`:** the docs example uses `'snowflake'` because the target is Snowflake Polaris. For Atmosphere's self-hosted Polaris the warehouse name is the **catalog name** — `'atmosphere'`. Confirmed from [infra/init/setup.py:25](infra/init/setup.py#L25) (`CATALOG_NAME = "atmosphere"`).
- **`auth_header`:** not shown in the docs example but supported. Needed for Polaris realm selection — `auth_header = 'Polaris-Realm:POLARIS'`. This mirrors the `Polaris-Realm` HTTP header used in [infra/init/setup.py:53,66](infra/init/setup.py#L53).
- **`vended_credentials = true`:** also not in the docs example but the standard pattern when Polaris vends S3 credentials at FileIO load time. Without it, ClickHouse would need direct RustFS credentials, defeating the Polaris auth model.

Final DDL we will issue from the init container:

```sql
CREATE DATABASE IF NOT EXISTS polaris_catalog
ENGINE = DataLakeCatalog('http://polaris:8181/api/catalog/v1')
SETTINGS
    catalog_type = 'rest',
    catalog_credential = '<CLICKHOUSE_CLIENT_ID>:<CLICKHOUSE_CLIENT_SECRET>',
    warehouse = 'atmosphere',
    auth_scope = 'PRINCIPAL_ROLE:ALL',
    auth_header = 'Polaris-Realm:POLARIS',
    oauth_server_uri = 'http://polaris:8181/api/catalog/v1/oauth/tokens',
    storage_endpoint = 'http://rustfs:9000',
    vended_credentials = true
```

## Nested namespace bug — does not affect Atmosphere

[Issue #93464](https://github.com/ClickHouse/ClickHouse/issues/93464) reports that `DataLakeCatalog` with REST catalogs URL-encodes nested namespace segments with `.` instead of the Iceberg-spec `%1F` separator, breaking queries against hierarchical namespaces.

**Impact on Atmosphere: none.** [infra/init/setup.py:26](infra/init/setup.py#L26) creates **flat single-level namespaces** (`raw`, `staging`, `core`, `mart`) inside the `atmosphere` catalog. There is no `atmosphere.mart` nested namespace — `mart` is a top-level namespace within the catalog. ClickHouse reaches it via `polaris_catalog.mart.mart_events_per_second`, where the Polaris REST call is `/api/catalog/v1/atmosphere/namespaces/mart/tables/mart_events_per_second`. Single level, no encoding issue.

If we ever add nested namespaces (e.g. `core.sentiment`) we would hit this bug and need a CH upgrade or flattening workaround.

## `users.xml` — readonly profile

[Changelog 2026](https://clickhouse.com/docs/whats-new/changelog) notes a new `use_partition_pruning` setting (alias of `use_iceberg_partition_pruning`), plus async metadata prefetch via `iceberg_metadata_async_prefetch_period_ms` and cached-metadata reads via `iceberg_metadata_staleness_ms`.

For the `atmosphere_reader` profile we only need:
- `readonly = 1` (profile-level)
- `use_iceberg_partition_pruning = 1` (query-time pruning)
- `changeable_in_readonly` on `max_execution_time` — **required** by the Go client used by the Grafana plugin, which attempts to `SET max_execution_time` on every query. Without this override the readonly user can't run any query.

Network restriction for `atmosphere_admin` user: use `<networks><ip>` rules inside `users.xml`. The Docker compose network subnets are allocated by Docker; the conservative pattern is `<ip>172.16.0.0/12</ip>` (covers the full Docker private range) plus `<ip>127.0.0.1</ip>` for local exec. The admin user is only reached by the `init` container on the `atmosphere-data` network so this is defence in depth; the actual access control is that init is the only caller and the container is rebuilt each `make up`.

## Grafana datasource provisioning YAML

From [ClickHouse's Grafana integration docs](https://clickhouse.com/docs/integrations/grafana/config):

```yaml
datasources:
  - name: ClickHouse
    uid: clickhouse
    type: grafana-clickhouse-datasource
    jsonData:
      host: clickhouse
      port: 9000
      protocol: native
      username: $__env{CLICKHOUSE_READER_USER}
      tlsSkipVerify: false
      defaultDatabase: polaris_catalog
    secureJsonData:
      password: $__env{CLICKHOUSE_READER_PASSWORD}
```

- Native protocol defaults: 9000 insecure, 9440 secure. We use 9000 on the internal Docker network; Cloudflare Tunnel handles TLS at the M7 swap.
- `$__env{VAR}` is the supported Grafana env substitution syntax for provisioning files.
- `defaultDatabase: polaris_catalog` lets panel SQL reference `mart.mart_events_per_second` without the `polaris_catalog.` prefix — but for clarity we'll still qualify in panel SQL since panel 6 also reads `raw.raw_events`.

Important: **plugin installation via `grafana cli plugins install` is being deprecated in February 2026.** The plan already bakes the plugin at `docker build` time via the existing `grafana/Dockerfile`, so we're on the supported path — but the Dockerfile line might need a URL-based install if the CLI install stops working for `grafana-clickhouse-datasource`. Verify during CH-04.

## Summary of decisions locked in by this research

| Item | Decision |
|---|---|
| Alpine image viability | Confirmed — wget + clickhouse-client present |
| Healthcheck | `wget -qO- http://localhost:8123/ping` |
| DDL `warehouse` value | `'atmosphere'` (not `'snowflake'`) |
| DDL `auth_header` | `'Polaris-Realm:POLARIS'` (required, not in docs example) |
| DDL `vended_credentials` | `true` (required for Polaris-vended S3 creds) |
| Experimental flag scope | Profile-wide via `config.d/datalake.xml` |
| Nested namespace bug | Not affecting us — namespaces are flat |
| Reader profile settings | `readonly=1`, `use_iceberg_partition_pruning=1`, `changeable_in_readonly` on `max_execution_time` |
| Grafana env substitution | `$__env{VAR}` |
| Plugin install method | Bake at image build via existing `grafana/Dockerfile` |
