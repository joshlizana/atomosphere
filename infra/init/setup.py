"""
Atmosphere init container — creates RustFS bucket, Polaris catalog, and Iceberg namespaces.
Idempotent: safe to run multiple times.
"""

import json
import os
import sys
import time

import boto3
import requests
from botocore.exceptions import ClientError

RUSTFS_ENDPOINT = os.environ.get("RUSTFS_ENDPOINT", "http://rustfs:9000")
RUSTFS_ACCESS_KEY = os.environ.get("RUSTFS_ROOT_USER", "atmosphere")
RUSTFS_SECRET_KEY = os.environ.get("RUSTFS_ROOT_PASSWORD", "atmosphere-secret-key")
BUCKET_NAME = "warehouse"

POLARIS_HOST = os.environ.get("POLARIS_HOST", "http://polaris:8181")
POLARIS_REALM = "POLARIS"
CLIENT_ID = os.environ.get("POLARIS_CLIENT_ID", "root")
CLIENT_SECRET = os.environ.get("POLARIS_CLIENT_SECRET", "s3cr3t")

CATALOG_NAME = "atmosphere"
NAMESPACES = ["raw", "staging", "core", "mart"]


def create_rustfs_bucket():
    s3 = boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
        region_name="us-east-1",
    )

    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"  bucket '{BUCKET_NAME}' already exists")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
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

    # Check if catalog exists
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
                "endpoint": RUSTFS_ENDPOINT,
                "endpointInternal": RUSTFS_ENDPOINT,
                "pathStyleAccess": True,
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

    # Grant CATALOG_MANAGE_CONTENT to catalog_admin role
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

    print("\n[1/5] Waiting for RustFS...")
    wait_for_service(f"{RUSTFS_ENDPOINT}/health", "RustFS")

    print("\n[2/5] Creating RustFS bucket...")
    create_rustfs_bucket()

    print("\n[3/5] Waiting for Polaris...")
    wait_for_service(f"{POLARIS_HOST.replace('8181', '8182')}/q/health", "Polaris")

    print("\n[4/5] Creating Polaris catalog...")
    token = get_polaris_token()
    create_catalog(token)

    print("\n[5/5] Creating Iceberg namespaces...")
    create_namespaces(token)

    print("\n=== Init complete ===")


if __name__ == "__main__":
    main()
