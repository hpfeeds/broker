FROM rust:1-buster as builder
WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends musl-tools ca-certificates
RUN rustup target add x86_64-unknown-linux-musl

COPY . /src
RUN cargo build --target x86_64-unknown-linux-musl --release

FROM scratch
STOPSIGNAL SIGINT
COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/hpfeeds-broker /hpfeeds-broker
USER 5000
CMD ["/hpfeeds-broker"]
