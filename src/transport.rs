/********************************************************************************
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

/*! Native owned-frame uProtocol transport API implementation for MQTT 5. */

use std::sync::Arc;

use async_trait::async_trait;
use up_rust::{
    transport::{UOwnedTransportImpl, ValidatedOwnedFrame},
    UCode, UOwnedListener, UStatus, UUri,
};

use crate::Mqtt5Transport;

#[async_trait]
impl UOwnedTransportImpl for Mqtt5Transport {
    async fn send_validated_owned(&self, frame: ValidatedOwnedFrame) -> Result<(), UStatus> {
        self.send_message(frame.metadata(), frame.payload().cloned())
            .await
    }

    async fn register_validated_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UOwnedListener>,
    ) -> Result<(), UStatus> {
        let topic = self
            .to_mqtt_topic_string(source_filter, sink_filter)
            .map_err(|e| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, e.to_string()))?;

        self.add_listener(&topic, listener).await
    }

    async fn unregister_validated_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UOwnedListener>,
    ) -> Result<(), UStatus> {
        let topic = self
            .to_mqtt_topic_string(source_filter, sink_filter)
            .map_err(|e| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, e.to_string()))?;

        self.remove_listener(&topic, listener).await
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use bytes::Bytes;
    use mockall::predicate::always;
    use protobuf::well_known_types::wrappers::StringValue;
    use tokio::sync::RwLock;
    use up_rust::{
        payload::{RawBytes, UDeserializer, UWireError},
        ProtobufPayload, UFrameMetadata, UOwnedFrame, UOwnedTransport,
    };

    use crate::{
        listener_registry::RegisteredListeners, mapping::MockMessageMapper,
        mqtt_client::MockMqttClientOperations, TransportMode,
    };

    use super::*;

    fn frame(source: &str, payload: &[u8]) -> UOwnedFrame {
        let source = UUri::from_str(source).expect("Expected a valid source value");
        UOwnedFrame::try_with_payload(
            UFrameMetadata::try_publish(source)
                .expect("valid publish metadata")
                .with_encoding(RawBytes::encoding()),
            Bytes::copy_from_slice(payload),
        )
        .expect("valid test frame")
    }

    #[tokio::test]
    async fn send_owned_publishes_native_frame_payload() {
        let message_to_send = frame("//vin.vehicles/A8000/2/8A50", b"payload");

        let mut client_operations = MockMqttClientOperations::new();
        client_operations
            .expect_publish()
            .once()
            .return_once(|mqtt_message| {
                assert_eq!(mqtt_message.topic(), "vin.vehicles/8000/A/2/8A50");
                assert_eq!(mqtt_message.payload(), b"payload");
                Ok(())
            });

        let mut message_mapper = MockMessageMapper::new();
        message_mapper
            .expect_create_mqtt_properties_from_frame_metadata()
            .with(always())
            .once()
            .returning(|header| {
                assert_eq!(header.encoding(), Some(&RawBytes::encoding()));
                Ok(paho_mqtt::Properties::new())
            });

        let mqtt_transport = Mqtt5Transport {
            mqtt_client: Arc::new(client_operations),
            registered_listeners: Arc::new(RwLock::new(RegisteredListeners::default())),
            message_mapper: Arc::new(message_mapper),
            authority_name: "test".to_string(),
            mode: TransportMode::InVehicle,
            message_callback_handle: None,
        };

        mqtt_transport.send_owned(message_to_send).await.unwrap();
    }

    #[tokio::test]
    async fn send_owned_propagates_publish_errors() {
        let message_to_send = frame("//vin.vehicles/A8000/2/8A50", b"payload");
        let expected_code = UCode::PERMISSION_DENIED;

        let mut client_operations = MockMqttClientOperations::new();
        client_operations
            .expect_publish()
            .once()
            .return_once(move |_| Err(UStatus::fail_with_code(expected_code, "denied")));

        let mut message_mapper = MockMessageMapper::new();
        message_mapper
            .expect_create_mqtt_properties_from_frame_metadata()
            .once()
            .returning(|_| Ok(paho_mqtt::Properties::new()));

        let mqtt_transport = Mqtt5Transport {
            mqtt_client: Arc::new(client_operations),
            registered_listeners: Arc::new(RwLock::new(RegisteredListeners::default())),
            message_mapper: Arc::new(message_mapper),
            authority_name: "test".to_string(),
            mode: TransportMode::InVehicle,
            message_callback_handle: None,
        };

        assert!(mqtt_transport
            .send_owned(message_to_send)
            .await
            .is_err_and(|err| err.get_code() == expected_code));
    }

    #[tokio::test]
    async fn send_owned_preserves_protobuf_payload_as_payload_only() {
        let source = UUri::from_str("//vin.vehicles/A8000/2/8A50").unwrap();
        let mut value = StringValue::new();
        value.value = "protobuf payload".to_string();
        let message_to_send = UOwnedFrame::from_serializable::<ProtobufPayload, _>(
            UFrameMetadata::try_publish(source).unwrap(),
            &value,
        )
        .unwrap();

        let mut client_operations = MockMqttClientOperations::new();
        client_operations
            .expect_publish()
            .once()
            .return_once(|mqtt_message| {
                let decoded = <StringValue as UDeserializer<ProtobufPayload>>::deserialize_from(
                    mqtt_message.payload(),
                )
                .unwrap();
                assert_eq!(decoded.value, "protobuf payload");
                Ok(())
            });

        let mut message_mapper = MockMessageMapper::new();
        message_mapper
            .expect_create_mqtt_properties_from_frame_metadata()
            .once()
            .returning(|header| {
                assert_eq!(header.encoding(), Some(&ProtobufPayload::encoding()));
                Ok(paho_mqtt::Properties::new())
            });

        let mqtt_transport = Mqtt5Transport {
            mqtt_client: Arc::new(client_operations),
            registered_listeners: Arc::new(RwLock::new(RegisteredListeners::default())),
            message_mapper: Arc::new(message_mapper),
            authority_name: "test".to_string(),
            mode: TransportMode::InVehicle,
            message_callback_handle: None,
        };

        mqtt_transport.send_owned(message_to_send).await.unwrap();
    }

    #[tokio::test]
    async fn send_owned_keeps_raw_payload_incompatible_with_protobuf_payload_codec() {
        let source = UUri::from_str("//vin.vehicles/A8000/2/8A50").unwrap();
        let message_to_send = UOwnedFrame::try_with_payload(
            UFrameMetadata::try_publish(source)
                .unwrap()
                .with_encoding(RawBytes::encoding()),
            [0x0a_u8].as_slice(),
        )
        .unwrap();

        let mut client_operations = MockMqttClientOperations::new();
        client_operations
            .expect_publish()
            .once()
            .return_once(|mqtt_message| {
                let result = <StringValue as UDeserializer<ProtobufPayload>>::deserialize_from(
                    mqtt_message.payload(),
                );
                assert!(matches!(result, Err(UWireError::InvalidPayload(_))));
                Ok(())
            });

        let mut message_mapper = MockMessageMapper::new();
        message_mapper
            .expect_create_mqtt_properties_from_frame_metadata()
            .once()
            .returning(|header| {
                assert_eq!(header.encoding(), Some(&RawBytes::encoding()));
                Ok(paho_mqtt::Properties::new())
            });

        let mqtt_transport = Mqtt5Transport {
            mqtt_client: Arc::new(client_operations),
            registered_listeners: Arc::new(RwLock::new(RegisteredListeners::default())),
            message_mapper: Arc::new(message_mapper),
            authority_name: "test".to_string(),
            mode: TransportMode::InVehicle,
            message_callback_handle: None,
        };

        mqtt_transport.send_owned(message_to_send).await.unwrap();
    }
}
