FROM eclipse-temurin:11-jdk-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends ant ant-optional \
    && rm -rf /var/lib/apt/lists/*
