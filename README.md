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

## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `hpfeeds-broker` by you, shall be licensed as MIT, without any
additional terms or conditions.
