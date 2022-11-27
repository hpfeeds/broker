# hpfeeds-broker

`hpfeeds-broker` is a Rust implementation of a HPFeeds event broker.

## Running

Start the server:

```
RUST_LOG=debug cargo run --bin hpfeeds-broker
```

The [`tracing`](https://github.com/tokio-rs/tracing) crate is used to provide structured logs.
You can substitute `debug` with the desired [log level][level].

[level]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives

## OpenTelemetry

If you are running many instances of your application (which is usually the case
when you are developing a cloud service, for example), you need a way to get all
of your trace data out of your host and into a centralized place. There are many
options here, such as Prometheus, Jaeger, DataDog, Honeycomb, AWS X-Ray etc.

We leverage OpenTelemetry, because it's an open standard that allows for a
single data format to be used for all the options mentioned above (and more).
This eliminates the risk of vendor lock-in, since you can switch between
providers if needed.

## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `hpfeeds-broker` by you, shall be licensed as MIT, without any
additional terms or conditions.
