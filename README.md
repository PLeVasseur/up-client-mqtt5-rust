# Eclipse uProtocol MQTT 5 Transport Library for Rust

This library provides a Rust based implementation of the [MQTT 5 uProtocol Transport v1.6.0-alpha.7](https://github.com/eclipse-uprotocol/up-spec/blob/v1.6.0-alpha.7/up-l1/mqtt_5.adoc).

## Getting Started

The [transport contract guide](docs/transport-contract.md) describes configuration,
opaque payload identity and TTL carriage. Public documentation is enforced with
`missing_docs = "deny"` and warning-denied Rustdoc/doctests.
Received encoding identifiers and opaque payload bytes are preserved, including
reserved values. Reconstruction uses the SDK received-declaration and checked
projection APIs; producer-only encoding restrictions are enforced when originating
messages, rather than when relaying received data.

Add the following to the `[dependencies]` section of your `Cargo.toml` file:

```toml
[dependencies]
up-rust = { version = "0.10" }
up-transport-mqtt5 = { version = "0.4" }
```

Please refer to [the crate's Rust Docs](https://docs.rs/up-transport-mqtt5/) and the [examples](./examples/) folder to see how to configure and use the transport.

## Building from Source

### Clone the Repository

```sh
git clone --recurse-submodules git@github.com:eclipse-uprotocol/up-transport-mqtt5-rust
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

For broker acceptance, first prepare `eclipse-mosquitto:2.0` on the Docker daemon
selected by `DOCKER_HOST`, then run `UP_MQTT5_REQUIRE_BROKER=1 cargo test --locked`.
The build script checks the local image without fetching it. Missing prerequisites
fail in this mode; all four authentication, authorization and reconnect cases must
execute. The [transport contract guide](docs/transport-contract.md) describes the
default optional detection and the payload/TTL mapping separately.

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
