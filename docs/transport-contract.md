# MQTT transport configuration and carriage

`Mqtt5TransportOptions` combines the transport mode and bounded listener/filter
limits with `MqttClientOptions`. Client options own broker connection, authentication,
session and TLS configuration. The transport operates on the application's Tokio
runtime and implements classic `UTransport` message carriage.

Payload encoding is preserved as an explicit uProtocol identity; opaque carriage
does not select a decoder. MQTT message expiry and uProtocol producer TTL are
distinct: positive original TTL is preserved in the user property, absent/zero TTL
means no uProtocol expiry, and MQTT expiry interval zero is never converted into an
unlimited lifetime. Mapping tests cover that distinction; real-broker tests exercise
authentication, authorization and reconnects with and without persistence.

Public documentation uses `missing_docs = "deny"`, with strict Rustdoc/doctests.
The build script detects whether the Mosquitto container image is available and
enables Docker-backed integration tests accordingly; that detection is not itself
a broker interoperability test. Broker validation requires the real configured setup.
