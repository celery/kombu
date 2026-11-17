FROM redis:latest

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV REDIS_CLUSTER_PORT=6370
EXPOSE 6370

ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
