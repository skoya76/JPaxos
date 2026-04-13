FROM eclipse-temurin:11-jdk-jammy AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends ant ant-optional \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN ant build

FROM eclipse-temurin:11-jdk-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash iproute2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/jpaxos

COPY --from=build /src/bin /opt/jpaxos/bin
COPY --from=build /src/lib /opt/jpaxos/lib
COPY --from=build /src/natives /opt/jpaxos/natives
COPY docker/e2e/paxos.e2e.properties /opt/jpaxos/paxos.properties
COPY docker/e2e/logback.e2e.xml /opt/jpaxos/logback.xml
COPY docker/e2e/jpaxos.sh /opt/jpaxos/docker/e2e/jpaxos.sh
COPY docker/e2e/entrypoint.sh /opt/jpaxos/docker/e2e/entrypoint.sh

RUN chmod +x /opt/jpaxos/docker/e2e/jpaxos.sh /opt/jpaxos/docker/e2e/entrypoint.sh

ENTRYPOINT ["/opt/jpaxos/docker/e2e/entrypoint.sh"]
