FROM debian:trixie-slim

ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY docker-bin/${TARGETARCH}/oxidemq /usr/local/bin/oxidemq

EXPOSE 9092 9093 8081 8082

ENV HOST=0.0.0.0
ENV KAFKA_PORT=9092
ENV SSL_PORT=9093
ENV SCHEMA_REGISTRY_PORT=8081
ENV ADMIN_PORT=8082

ENTRYPOINT ["/usr/local/bin/oxidemq"]
CMD ["start"]
