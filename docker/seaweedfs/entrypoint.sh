#!/bin/sh
set -eu

: "${SEAWEEDFS_ADMIN_USER:?SEAWEEDFS_ADMIN_USER is required}"
: "${SEAWEEDFS_ADMIN_PASSWORD:?SEAWEEDFS_ADMIN_PASSWORD is required}"
: "${SEAWEEDFS_READER_USER:?SEAWEEDFS_READER_USER is required}"
: "${SEAWEEDFS_READER_PASSWORD:?SEAWEEDFS_READER_PASSWORD is required}"

mkdir -p /etc/seaweedfs
envsubst < /etc/seaweedfs/s3.json.tmpl > /etc/seaweedfs/s3.json
chmod 600 /etc/seaweedfs/s3.json

exec weed server \
  -dir=/data \
  -s3 \
  -s3.port=8333 \
  -s3.config=/etc/seaweedfs/s3.json \
  -master.volumeSizeLimitMB=1024 \
  -volume.max=0
