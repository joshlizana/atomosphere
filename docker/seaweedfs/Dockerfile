FROM chrislusf/seaweedfs:4.19

# gettext-base provides envsubst so the entrypoint can render s3.json from env
# vars at startup. No hardcoded credentials live in the image.
USER root
RUN apk add --no-cache gettext

COPY s3.json.tmpl /etc/seaweedfs/s3.json.tmpl
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
