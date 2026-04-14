FROM grafana/grafana-oss:latest

USER root
RUN mkdir -p /opt/grafana/plugins && \
    grafana cli --pluginsDir /opt/grafana/plugins plugins install grafana-clickhouse-datasource && \
    chown -R grafana:root /opt/grafana
USER grafana

ENV GF_PATHS_PLUGINS=/opt/grafana/plugins
