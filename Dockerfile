FROM rust:alpine3.16 as builder
WORKDIR /src

RUN apk --no-cache add musl-dev
COPY . /src
RUN cargo build --target x86_64-unknown-linux-musl --release

FROM scratch
STOPSIGNAL SIGINT
COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/hpfeeds-broker /hpfeeds-broker
CMD ["/hpfeeds-broker"]
