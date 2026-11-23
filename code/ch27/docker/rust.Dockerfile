# Rust toolchain for the book's Connect clients: the Rust client compiles the Spark Connect
# .proto files at build time, so it needs protoc and the well-known .proto files (libprotobuf-dev). cmake is for DuckDB (Chapter 83).
FROM rust:bookworm
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev cmake \
    && rm -rf /var/lib/apt/lists/*
