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
# Received encoding and reconstruction

MQTT Content Type carries the exact checked16-bit payload-encoding identifier.
Received unknown, private and reserved identities remain opaque carriage; the
SDK's producer-only restriction on originating reserved IDs does not discard them.
The mapper validates common message attributes, restores the received declaration
and uses checked attribute/frame/message projections to combine metadata and bytes.
Payload absence remains distinct from a present-empty payload. TTL mapping and
the broker-expiry rules remain separate from this reconstruction policy.
