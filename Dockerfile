FROM rust:1-buster as builder
WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends musl-tools ca-certificates
RUN rustup target add x86_64-unknown-linux-musl

COPY . /src
RUN cargo build --target x86_64-unknown-linux-musl --release

FROM scratch
STOPSIGNAL SIGINT
RUN mkdir -p /app/bin
COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/hpfeeds-broker /app/bin/hpfeeds-broker
USER 5000
CMD ["/app/bin/hpfeeds-broker", "--endpoint=tcp:port=20000", "--exporter=0.0.0.0:9431"]