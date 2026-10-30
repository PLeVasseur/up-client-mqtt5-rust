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
The build script checks whether the selected Docker daemon already has the
`eclipse-mosquitto:2.0` image, without pulling it, and
enables Docker-backed integration tests accordingly; that detection is not itself
a broker interoperability test. Broker validation requires the real configured setup.
Set `UP_MQTT5_REQUIRE_BROKER=1` for broker acceptance: a missing daemon/image then
fails the build instead of marking tests ignored. `DOCKER_HOST` selects the daemon;
prepare its image separately before running tests. Changing either setting reruns
the build-script availability check. Ordinary builds can use the default optional
detection, or explicitly set `UP_MQTT5_REQUIRE_BROKER=0`.
`python3 tools/test_broker_detection.py` checks the build configuration's positive,
missing-prerequisite and invalid-setting behavior. Its controlled CLI responses
are unit-test inputs, not broker validation. The CI test job prepares the actual
image separately and runs in required-broker mode.

## Received encoding and reconstruction

MQTT Content Type carries the exact checked 16-bit payload-encoding identifier.
Received unknown, private and reserved identities remain opaque carriage; the
SDK's producer-only restriction on originating reserved IDs does not discard them.
The mapper validates common message attributes, restores the received declaration
and uses checked attribute/frame/message projections to combine metadata and bytes.
Payload absence remains distinct from a present-empty payload. TTL mapping and
the broker-expiry rules remain separate from this reconstruction policy.
The SDK dependency explicitly enables `up-core-api` for the protobuf-backed
message/attribute conversions used by MQTT. Opaque carriage itself does not require
the SDK's selected-wire adapter or native agreement features.

## Listener registration

Registering the same listener for the same filter again has the effect of a single
registration: it succeeds without further changes, also when the filter's listener
limit has been reached, while a distinct listener then fails with `RESOURCE_EXHAUSTED`.
Registering the first listener for a filter makes the transport subscribe to the filter
with the broker. If the registration future is dropped before the broker has responded,
the registration has no effect: the filter, listener and subscription identifier are
removed again, and the transport withdraws the subscription, which the broker may still
establish, before any later registration subscribes. A cancelled registration can be
retried like a failed one.
