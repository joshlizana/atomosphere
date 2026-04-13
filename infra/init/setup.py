"""
Atmosphere init container — creates the SeaweedFS warehouse bucket, Polaris
catalog, Iceberg namespaces, and the ClickHouse read-only principal +
DataLakeCatalog database. Idempotent: safe to run multiple times.
"""

import json
import os
import sys
import time

import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
BUCKET_NAME = "warehouse"

POLARIS_HOST = os.environ["POLARIS_HOST"]
POLARIS_REALM = os.environ["POLARIS_REALM"]
CLIENT_ID = os.environ["POLARIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["POLARIS_CLIENT_SECRET"]

CATALOG_NAME = "atmosphere"
NAMESPACES = ["raw", "staging", "core"]

CLICKHOUSE_HOST = os.environ["CLICKHOUSE_HOST"]
CLICKHOUSE_ADMIN_USER = os.environ["CLICKHOUSE_ADMIN_USER"]
CLICKHOUSE_ADMIN_PASSWORD = os.environ["CLICKHOUSE_ADMIN_PASSWORD"]

CH_PRINCIPAL = "clickhouse"
CH_PRINCIPAL_ROLE = "clickhouse_reader"
CH_CATALOG_ROLE = "atmosphere_reader"
CH_CATALOG_GRANTS = [
    "TABLE_READ_DATA",
    "TABLE_READ_PROPERTIES",
    "TABLE_LIST",
    "NAMESPACE_LIST",
    "NAMESPACE_READ_PROPERTIES",
    "CATALOG_READ_PROPERTIES",
]
CH_CREDS_DIR = "/var/polaris-creds"
CH_CREDS_FILE = f"{CH_CREDS_DIR}/clickhouse.json"


def create_warehouse_bucket():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
        ),
    )

    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"  bucket '{BUCKET_NAME}' already exists")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket", "NotFound"):
            s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"  bucket '{BUCKET_NAME}' created")
        else:
            raise


def get_polaris_token():
    resp = requests.post(
        f"{POLARIS_HOST}/api/catalog/v1/oauth/tokens",
        auth=(CLIENT_ID, CLIENT_SECRET),
        headers={"Polaris-Realm": POLARIS_REALM},
        data={"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"},
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    return token


def polaris_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Polaris-Realm": POLARIS_REALM,
    }


def create_catalog(token):
    headers = polaris_headers(token)

    resp = requests.get(
        f"{POLARIS_HOST}/api/management/v1/catalogs/{CATALOG_NAME}",
        headers=headers,
    )
    if resp.status_code == 200:
        print(f"  catalog '{CATALOG_NAME}' already exists")
        return

    payload = {
        "catalog": {
            "name": CATALOG_NAME,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {
                "default-base-location": f"s3://{BUCKET_NAME}/",
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [f"s3://{BUCKET_NAME}/"],
                "endpoint": S3_ENDPOINT,
                "endpointInternal": S3_ENDPOINT,
                "pathStyleAccess": True,
                # SeaweedFS doesn't implement AWS STS AssumeRole, so Polaris
                # must skip the subscoping step and hand clients the raw
                # storage credentials (the SEAWEEDFS_ADMIN_* identity). Without
                # this, every request crashes with "Failed to get subscoped
                # credentials: (Service: Sts, Status Code: 503...)".
                "stsUnavailable": True,
            },
        }
    }

    resp = requests.post(
        f"{POLARIS_HOST}/api/management/v1/catalogs",
        headers=headers,
        json=payload,
    )
    resp.raise_for_status()
    print(f"  catalog '{CATALOG_NAME}' created")

    resp = requests.put(
        f"{POLARIS_HOST}/api/management/v1/catalogs/{CATALOG_NAME}/catalog-roles/catalog_admin/grants",
        headers=headers,
        json={"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"},
    )
    resp.raise_for_status()
    print("  granted CATALOG_MANAGE_CONTENT to catalog_admin")


def create_namespaces(token):
    headers = polaris_headers(token)

    for ns in NAMESPACES:
        resp = requests.get(
            f"{POLARIS_HOST}/api/catalog/v1/{CATALOG_NAME}/namespaces/{ns}",
            headers=headers,
        )
        if resp.status_code == 200:
            print(f"  namespace '{CATALOG_NAME}.{ns}' already exists")
            continue

        resp = requests.post(
            f"{POLARIS_HOST}/api/catalog/v1/{CATALOG_NAME}/namespaces",
            headers=headers,
            json={"namespace": [ns]},
        )
        resp.raise_for_status()
        print(f"  namespace '{CATALOG_NAME}.{ns}' created")


def create_clickhouse_reader(token):
    """Provision the ClickHouse read-only principal + RBAC chain.

    Polaris generates clientId/clientSecret server-side on principal creation
    and returns them once. We persist the creds to CH_CREDS_FILE for idempotent
    reuse across restarts. If the file is missing but the principal exists
    (e.g. fresh init volume on an existing Polaris), we rotate to recover.
    """
    headers = polaris_headers(token)

    os.makedirs(CH_CREDS_DIR, exist_ok=True)

    principal_url = f"{POLARIS_HOST}/api/management/v1/principals/{CH_PRINCIPAL}"
    if os.path.exists(CH_CREDS_FILE):
        with open(CH_CREDS_FILE) as f:
            creds = json.load(f)
        print(f"  principal '{CH_PRINCIPAL}' creds loaded from {CH_CREDS_FILE}")
    else:
        resp = requests.post(
            f"{POLARIS_HOST}/api/management/v1/principals",
            headers=headers,
            json={
                "principal": {"name": CH_PRINCIPAL},
                "credentialRotationRequired": False,
            },
        )
        if resp.status_code == 409:
            print(f"  principal '{CH_PRINCIPAL}' exists without local creds — rotating")
            resp = requests.post(
                f"{principal_url}/rotate",
                headers=headers,
            )
            resp.raise_for_status()
        else:
            resp.raise_for_status()
            print(f"  principal '{CH_PRINCIPAL}' created")

        body = resp.json()
        creds = {
            "clientId": body["credentials"]["clientId"],
            "clientSecret": body["credentials"]["clientSecret"],
        }
        with open(CH_CREDS_FILE, "w") as f:
            json.dump(creds, f)
        os.chmod(CH_CREDS_FILE, 0o600)
        print(f"  creds written to {CH_CREDS_FILE}")

    # Principal role
    pr_url = f"{POLARIS_HOST}/api/management/v1/principal-roles"
    resp = requests.get(f"{pr_url}/{CH_PRINCIPAL_ROLE}", headers=headers)
    if resp.status_code != 200:
        resp = requests.post(
            pr_url,
            headers=headers,
            json={"principalRole": {"name": CH_PRINCIPAL_ROLE}},
        )
        resp.raise_for_status()
        print(f"  principal role '{CH_PRINCIPAL_ROLE}' created")
    else:
        print(f"  principal role '{CH_PRINCIPAL_ROLE}' already exists")

    # Catalog role
    cr_url = f"{POLARIS_HOST}/api/management/v1/catalogs/{CATALOG_NAME}/catalog-roles"
    resp = requests.get(f"{cr_url}/{CH_CATALOG_ROLE}", headers=headers)
    if resp.status_code != 200:
        resp = requests.post(
            cr_url,
            headers=headers,
            json={"catalogRole": {"name": CH_CATALOG_ROLE}},
        )
        resp.raise_for_status()
        print(f"  catalog role '{CH_CATALOG_ROLE}' created")
    else:
        print(f"  catalog role '{CH_CATALOG_ROLE}' already exists")

    # Grants — catalog-level cascades to every namespace.
    # Polaris returns HTTP 500 + "duplicate key" on re-grants; treat as success.
    for privilege in CH_CATALOG_GRANTS:
        resp = requests.put(
            f"{cr_url}/{CH_CATALOG_ROLE}/grants",
            headers=headers,
            json={"type": "catalog", "privilege": privilege},
        )
        if resp.status_code in (200, 201):
            continue
        if resp.status_code == 500 and "duplicate key" in resp.text:
            continue
        resp.raise_for_status()
    print(f"  granted {len(CH_CATALOG_GRANTS)} privileges to '{CH_CATALOG_ROLE}'")

    # Assign principal role → principal (Polaris: 500+duplicate key on re-assign)
    resp = requests.put(
        f"{principal_url}/principal-roles",
        headers=headers,
        json={"principalRole": {"name": CH_PRINCIPAL_ROLE}},
    )
    if not (resp.status_code in (200, 201) or (resp.status_code == 500 and "duplicate key" in resp.text)):
        resp.raise_for_status()
    print(f"  assigned '{CH_PRINCIPAL_ROLE}' → '{CH_PRINCIPAL}'")

    # Assign catalog role → principal role
    resp = requests.put(
        f"{POLARIS_HOST}/api/management/v1/principal-roles/{CH_PRINCIPAL_ROLE}/catalog-roles/{CATALOG_NAME}",
        headers=headers,
        json={"catalogRole": {"name": CH_CATALOG_ROLE}},
    )
    if not (resp.status_code in (200, 201) or (resp.status_code == 500 and "duplicate key" in resp.text)):
        resp.raise_for_status()
    print(f"  assigned '{CH_CATALOG_ROLE}' → '{CH_PRINCIPAL_ROLE}'")

    return creds


def wait_for_clickhouse():
    wait_for_service(f"{CLICKHOUSE_HOST}/ping", "ClickHouse")


def create_clickhouse_database(creds):
    # DROP + CREATE so setting changes (e.g. vended_credentials) always take
    # effect. The database holds no data — it's just the DataLakeCatalog
    # engine pointer to Polaris, so recreating is cheap and idempotent.
    statements = [
        "DROP DATABASE IF EXISTS polaris_catalog",
        f"""CREATE DATABASE polaris_catalog
ENGINE = DataLakeCatalog('{POLARIS_HOST}/api/catalog/v1')
SETTINGS
    catalog_type = 'rest',
    catalog_credential = '{creds["clientId"]}:{creds["clientSecret"]}',
    warehouse = '{CATALOG_NAME}',
    auth_scope = 'PRINCIPAL_ROLE:ALL',
    auth_header = 'Polaris-Realm:{POLARIS_REALM}',
    oauth_server_uri = '{POLARIS_HOST}/api/catalog/v1/oauth/tokens',
    storage_endpoint = '{S3_ENDPOINT}/{BUCKET_NAME}',
    vended_credentials = false
""",
    ]
    for stmt in statements:
        resp = requests.post(
            CLICKHOUSE_HOST,
            params={
                "user": CLICKHOUSE_ADMIN_USER,
                "password": CLICKHOUSE_ADMIN_PASSWORD,
                "allow_database_iceberg": "1",
                "allow_experimental_database_unity_catalog": "1",
            },
            data=stmt,
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"  ERROR: ClickHouse DDL failed ({resp.status_code})")
            print(f"  stmt: {stmt[:80]}")
            print(f"  response: {resp.text}")
            sys.exit(1)
    print("  database 'polaris_catalog' recreated on ClickHouse")


def wait_for_s3(endpoint, name, retries=30, delay=2):
    """SeaweedFS S3 gateway has no /health route. A 403/400 from a probe
    request still means the gateway is up and signing requests."""
    for i in range(retries):
        try:
            resp = requests.get(endpoint, timeout=5)
            if resp.status_code in (200, 400, 403, 404):
                return
        except requests.ConnectionError:
            pass
        print(f"  waiting for {name}... ({i + 1}/{retries})")
        time.sleep(delay)
    print(f"  ERROR: {name} not available after {retries * delay}s")
    sys.exit(1)


def wait_for_service(url, name, retries=30, delay=2):
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        print(f"  waiting for {name}... ({i + 1}/{retries})")
        time.sleep(delay)
    print(f"  ERROR: {name} not available after {retries * delay}s")
    sys.exit(1)


def main():
    print("=== Atmosphere Init ===")

    print("\n[1/7] Waiting for SeaweedFS...")
    # SeaweedFS S3 gateway responds 403 to anonymous bucket-list (correct
    # behavior with auth enabled), so that's an acceptable readiness signal.
    wait_for_s3(S3_ENDPOINT, "SeaweedFS")

    print("\n[2/7] Creating warehouse bucket...")
    create_warehouse_bucket()

    print("\n[3/7] Waiting for Polaris...")
    wait_for_service(f"{POLARIS_HOST.replace('8181', '8182')}/q/health", "Polaris")

    print("\n[4/7] Creating Polaris catalog...")
    token = get_polaris_token()
    create_catalog(token)

    print("\n[5/7] Creating Iceberg namespaces...")
    create_namespaces(token)

    print("\n[6/7] Provisioning ClickHouse Polaris principal + RBAC...")
    creds = create_clickhouse_reader(token)

    print("\n[7/7] Waiting for ClickHouse, then creating polaris_catalog database...")
    wait_for_clickhouse()
    create_clickhouse_database(creds)

    print("\n=== Init complete ===")


if __name__ == "__main__":
    main()
