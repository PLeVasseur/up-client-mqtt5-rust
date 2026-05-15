# Eclipse uProtocol MQTT 5 Transport Library for Rust

This library provides a Rust based implementation of the [MQTT 5 uProtocol Transport v1.6.0-alpha.7](https://github.com/eclipse-uprotocol/up-spec/blob/v1.6.0-alpha.7/up-l1/mqtt_5.adoc).

## Getting Started

Add the following to the `[dependencies]` section of your `Cargo.toml` file:

```toml
[dependencies]
up-rust = { version = "0.9" }
up-transport-mqtt5 = { version = "0.4" }
```

Please refer to [the crate's Rust Docs](https://docs.rs/up-transport-mqtt5/) and the [examples](./examples/) folder to see how to configure and use the transport.

This branch uses the native-frame `up-rust` transport API. `Mqtt5Transport` implements `UOwnedTransport`: it sends `UOwnedFrame` values and reconstructs native `UFrameMetadata` on receive. `UEncoding.content_type` maps to the MQTT 5 Content Type property, while `UEncoding.format_id` and `UEncoding.schema_ref` are preserved as user properties when present. The transport is owned-buffer only; MQTT broker delivery does not provide true zero-copy transmit loans or receive leases.

## Building from Source

### Clone the Repository

```sh
git clone --recurse-submodules git@github.com:eclipse-uprotocol/up-rust
```

The `--recurse-submodules` parameter is important to make sure that the git submodule referring to the uProtocol specification is being initialized in the workspace. The files contained in that submodule define uProtocol's behavior and are used to trace requirements to implementation and test as part of CI workflows.
If the repository has already been cloned without the parameter, the submodule can be initialized manually using `git submodule update --init --recursive`.

In order to make sure that you pull in any subsequent changes made to submodules from upstream, you need to use

```sh
git pull --recurse-submodules
```

If you want to make Git always pull with `--recurse-submodules`, you can set the configuration option *submodule.recurse* to `true` (this works for git pull since Git 2.15). This option will make Git use the `--recurse-submodules` flag for all commands that support it (except *clone*).

### Building the Library

To build the library, run `cargo build` in the project root directory.

### Running the Tests

To run the tests from the repo root directory, run
```bash
cargo test
```

### Running the Examples

The examples show how the transport can be used to publish uProtocol messages from one uEntity and consume these messages on another uEntity.

1. Start the Eclipse Mosquitto MQTT broker using Docker Compose:

   ```bash
   docker compose -f tests/mosquitto/docker-compose.yaml up --detach
   ```

2. Run the Subscriber with options appropriate for your MQTT broker.
   When using the Mosquitto broker started via Docker Compose, then the defaults should work:

   ```bash
   cargo run --example subscriber_example
   ```

   The Subscriber also supports configuration via command line options and/or environment variables:

   ```bash
   cargo run --example subscriber_example -- --help
   ```

3. Run the Publisher with options appropriate for your MQTT broker.
   When using the Mosquitto broker started via Docker Compose, then the defaults should work:

   ```bash
   cargo run --example publisher_example
   ```

   The Publisher also supports configuration via command line options and/or environment variables:

   ```bash
   cargo run --example publisher_example -- --help
   ```
