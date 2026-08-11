/********************************************************************************
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
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

use std::str::FromStr;

use bytes::Bytes;
use up_rust::{
    BuilderState, PayloadEncoding, UAttributes, UAttributesValidators, UCode, UMessage,
    UMessageBuilder, UMessageType, UPriority, UStatus, UUri, UUID,
};

const CURRENT_UPROTOCOL_MAJOR_VERSION: u8 = 1;

/// Constants defining the protobuf field numbers for `UAttributes`.
const KEY_UPROTOCOL_VERSION: &str = "uP";
const KEY_MESSAGE_ID: &str = "1";
const KEY_TYPE: &str = "2";
const KEY_SOURCE: &str = "3";
const KEY_SINK: &str = "4";
const KEY_PRIORITY: &str = "5";
const KEY_TTL: &str = "6";
const KEY_PERMISSION_LEVEL: &str = "7";
const KEY_COMMSTATUS: &str = "8";
const KEY_TOKEN: &str = "10";
const KEY_TRACEPARENT: &str = "11";

const RETIRED_PAYLOAD_KEYS: [&str; 3] = ["13", "14", "15"];

fn invalid_argument(message: impl Into<String>) -> UStatus {
    UStatus::fail_with_code(UCode::InvalidArgument, message)
}

fn missing_required_attribute(name: &str) -> UStatus {
    invalid_argument(format!("message attributes missing required {name}"))
}

/// Adds a user property to MQTT properties.
fn add_user_property(
    properties: &mut paho_mqtt::Properties,
    key: &str,
    value: &str,
    error_message: &str,
) -> Result<(), UStatus> {
    properties
        .push_string_pair(paho_mqtt::PropertyCode::UserProperty, key, value)
        .map_err(|e| UStatus::fail_with_code(UCode::Internal, format!("{error_message}: {e:?}")))
}

fn configure_common_attributes<S: BuilderState>(
    builder: &mut UMessageBuilder<S>,
    id: &UUID,
    priority: Option<UPriority>,
    ttl: Option<u32>,
    traceparent: Option<&str>,
) {
    builder.with_message_id(id.clone());
    if let Some(priority) = priority {
        builder.with_priority(priority);
    }
    if let Some(ttl) = ttl {
        builder.with_ttl(ttl);
    }
    if let Some(traceparent) = traceparent {
        builder.with_traceparent(traceparent);
    }
}

fn finish_message<S: BuilderState>(
    builder: &mut UMessageBuilder<S>,
    payload: Option<Bytes>,
    payload_encoding: Option<PayloadEncoding>,
) -> Result<UMessage, UStatus> {
    match (payload, payload_encoding) {
        (None, None) => builder.build(),
        (Some(payload), Some(payload_encoding)) => {
            builder.build_with_payload(payload, payload_encoding)
        }
        (Some(_), None) => {
            return Err(invalid_argument(
                "message payload has no payload-encoding identifier",
            ))
        }
        (None, Some(_)) => {
            return Err(invalid_argument(
                "message declares a payload-encoding identifier without payload bytes",
            ))
        }
    }
    .map_err(|e| invalid_argument(e.to_string()))
}

pub(crate) fn create_umessage_from_uattributes(
    attributes: &UAttributes,
    payload: Option<Bytes>,
) -> Result<UMessage, UStatus> {
    let payload_encoding = attributes.payload_encoding();
    match attributes.type_() {
        UMessageType::Publish => {
            let mut builder = UMessageBuilder::publish(attributes.source().clone());
            configure_common_attributes(
                &mut builder,
                attributes.id(),
                attributes.priority(),
                attributes.ttl(),
                attributes.traceparent(),
            );
            finish_message(&mut builder, payload, payload_encoding)
        }
        UMessageType::Notification => {
            let mut builder = UMessageBuilder::notification(
                attributes.source().clone(),
                attributes
                    .sink()
                    .ok_or_else(|| missing_required_attribute("sink"))?
                    .clone(),
            );
            configure_common_attributes(
                &mut builder,
                attributes.id(),
                attributes.priority(),
                attributes.ttl(),
                attributes.traceparent(),
            );
            finish_message(&mut builder, payload, payload_encoding)
        }
        UMessageType::Request => {
            let mut builder = UMessageBuilder::request(
                attributes
                    .sink()
                    .ok_or_else(|| missing_required_attribute("sink"))?
                    .clone(),
                attributes.source().clone(),
                attributes
                    .ttl()
                    .ok_or_else(|| missing_required_attribute("ttl"))?,
            );
            configure_common_attributes(
                &mut builder,
                attributes.id(),
                attributes.priority(),
                attributes.ttl(),
                attributes.traceparent(),
            );
            if let Some(token) = attributes.token() {
                builder.with_token(token);
            }
            if let Some(permission_level) = attributes.permission_level() {
                builder.with_permission_level(permission_level);
            }
            finish_message(&mut builder, payload, payload_encoding)
        }
        UMessageType::Response => {
            let mut builder = UMessageBuilder::response(
                attributes
                    .sink()
                    .ok_or_else(|| missing_required_attribute("sink"))?
                    .clone(),
                attributes
                    .request_id()
                    .ok_or_else(|| missing_required_attribute("request id"))?
                    .clone(),
                attributes.source().clone(),
            );
            configure_common_attributes(
                &mut builder,
                attributes.id(),
                attributes.priority(),
                attributes.ttl(),
                attributes.traceparent(),
            );
            if let Some(commstatus) = attributes.commstatus() {
                builder.with_comm_status(commstatus);
            }
            finish_message(&mut builder, payload, payload_encoding)
        }
    }
}

#[cfg_attr(test, mockall::automock)]
pub(crate) trait MessageMapper: Send + Sync {
    /// Creates MQTT 5 header properties from uProtocol message metadata.
    fn create_mqtt_properties_from_uattributes(
        &self,
        attributes: &UAttributes,
    ) -> Result<paho_mqtt::Properties, UStatus>;

    /// Creates uProtocol message metadata from MQTT header properties.
    fn create_uattributes_from_mqtt_properties(
        &self,
        props: &paho_mqtt::Properties,
    ) -> Result<UAttributes, UStatus>;
}

#[derive(Default)]
pub(crate) struct DefaultMessageMapper;

impl MessageMapper for DefaultMessageMapper {
    // [impl->dsn~up-transport-mqtt5-attributes-mapping~1]
    fn create_mqtt_properties_from_uattributes(
        &self,
        attributes: &UAttributes,
    ) -> Result<paho_mqtt::Properties, UStatus> {
        UAttributesValidators::validator_for_attributes(attributes)
            .validate(attributes)
            .map_err(|e| invalid_argument(format!("Invalid uAttributes, err: {e:?}")))?;

        let mut properties = paho_mqtt::Properties::new();
        add_user_property(
            &mut properties,
            KEY_UPROTOCOL_VERSION,
            &CURRENT_UPROTOCOL_MAJOR_VERSION.to_string(),
            "Failed to add uProtocol major version to MQTT User Properties",
        )?;

        if let Some(ttl) = attributes.ttl() {
            properties
                .push_u32(
                    paho_mqtt::PropertyCode::MessageExpiryInterval,
                    ttl.div_ceil(1000),
                )
                .map_err(|e| {
                    UStatus::fail_with_code(
                        UCode::Internal,
                        format!("Failed to create Message Expiry Interval property: {e:?}"),
                    )
                })?;
            if ttl % 1000 > 0 {
                add_user_property(
                    &mut properties,
                    KEY_TTL,
                    &ttl.to_string(),
                    "Failed to add TTL to MQTT User Properties",
                )?;
            }
        }

        add_user_property(
            &mut properties,
            KEY_MESSAGE_ID,
            &attributes.id().to_hyphenated_string(),
            "Failed to add message ID to MQTT User Properties",
        )?;
        add_user_property(
            &mut properties,
            KEY_TYPE,
            &attributes.type_().to_cloudevent_type(),
            "Failed to add message type to MQTT User Properties",
        )?;
        add_user_property(
            &mut properties,
            KEY_SOURCE,
            &attributes.source().to_uri(false),
            "Failed to add message source to MQTT User Properties",
        )?;

        if let Some(sink) = attributes.sink() {
            add_user_property(
                &mut properties,
                KEY_SINK,
                &sink.to_uri(false),
                "Failed to add message sink to MQTT User Properties",
            )?;
        }
        if let Some(priority) = attributes.priority() {
            add_user_property(
                &mut properties,
                KEY_PRIORITY,
                priority.to_priority_code(),
                "Failed to add message priority to MQTT User Properties",
            )?;
        }
        if let Some(permission_level) = attributes.permission_level() {
            add_user_property(
                &mut properties,
                KEY_PERMISSION_LEVEL,
                &permission_level.to_string(),
                "Failed to add message permission level to MQTT User Properties",
            )?;
        }
        if let Some(commstatus) = attributes.commstatus() {
            add_user_property(
                &mut properties,
                KEY_COMMSTATUS,
                &commstatus.value().to_string(),
                "Failed to add message comm status to MQTT User Properties",
            )?;
        }
        if let Some(request_id) = attributes.request_id() {
            properties
                .push_binary::<Vec<u8>>(paho_mqtt::PropertyCode::CorrelationData, request_id.into())
                .map_err(|e| {
                    UStatus::fail_with_code(
                        UCode::Internal,
                        format!("Failed to create Correlation Data property: {e:?}"),
                    )
                })?;
        }
        if let Some(token) = attributes.token() {
            add_user_property(
                &mut properties,
                KEY_TOKEN,
                token,
                "Failed to add message token to MQTT User Properties",
            )?;
        }
        if let Some(traceparent) = attributes.traceparent() {
            add_user_property(
                &mut properties,
                KEY_TRACEPARENT,
                traceparent,
                "Failed to add message traceparent to MQTT User Properties",
            )?;
        }

        if let Some(payload_encoding) = attributes.payload_encoding() {
            properties
                .push_string(
                    paho_mqtt::PropertyCode::ContentType,
                    &payload_encoding.id().to_string(),
                )
                .map_err(|e| {
                    UStatus::fail_with_code(
                        UCode::Internal,
                        format!("Failed to create Content Type property: {e:?}"),
                    )
                })?;
        }

        Ok(properties)
    }

    // [impl->dsn~up-transport-mqtt5-attributes-mapping~1]
    fn create_uattributes_from_mqtt_properties(
        &self,
        props: &paho_mqtt::Properties,
    ) -> Result<UAttributes, UStatus> {
        for key in RETIRED_PAYLOAD_KEYS {
            if props.find_user_property(key).is_some() {
                return Err(invalid_argument(format!(
                    "MQTT message contains retired payload metadata UserProperty {key}"
                )));
            }
        }

        let uprotocol_major_version = props
            .find_user_property(KEY_UPROTOCOL_VERSION)
            .ok_or_else(|| {
                invalid_argument("MQTT message does not contain uProtocol version identifier")
            })?
            .parse::<u8>()
            .map_err(|err| {
                invalid_argument(format!(
                    "Failed to map UserProperty {KEY_UPROTOCOL_VERSION} to uProtocol major version: {err}"
                ))
            })?;
        if uprotocol_major_version != CURRENT_UPROTOCOL_MAJOR_VERSION {
            return Err(invalid_argument(format!(
                "MQTT message contains unsupported uProtocol major version [expected: {CURRENT_UPROTOCOL_MAJOR_VERSION}, found: {uprotocol_major_version}]"
            )));
        }

        let id = props
            .find_user_property(KEY_MESSAGE_ID)
            .ok_or_else(|| missing_required_attribute("message id"))
            .and_then(|value| {
                UUID::from_str(&value).map_err(|e| {
                    invalid_argument(format!(
                        "Failed to map UserProperty {KEY_MESSAGE_ID} to Message ID: {e}"
                    ))
                })
            })?;
        let message_type = props
            .find_user_property(KEY_TYPE)
            .ok_or_else(|| missing_required_attribute("message type"))
            .and_then(|value| {
                UMessageType::try_from_cloudevent_type(value).map_err(|e| {
                    invalid_argument(format!(
                        "Failed to map UserProperty {KEY_TYPE} to Message Type: {e}"
                    ))
                })
            })?;
        let source = props
            .find_user_property(KEY_SOURCE)
            .ok_or_else(|| missing_required_attribute("source"))
            .and_then(|value| {
                UUri::from_str(&value).map_err(|e| {
                    invalid_argument(format!(
                        "Failed to map UserProperty {KEY_SOURCE} to Message Source: {e}"
                    ))
                })
            })?;
        let sink = props
            .find_user_property(KEY_SINK)
            .map(|value| {
                UUri::from_str(&value).map_err(|e| {
                    invalid_argument(format!(
                        "Failed to map UserProperty {KEY_SINK} to Message Sink: {e}"
                    ))
                })
            })
            .transpose()?;
        let priority = props
            .find_user_property(KEY_PRIORITY)
            .map(|value| {
                UPriority::try_from_priority_code(value).map_err(|e| {
                    invalid_argument(format!(
                        "Failed to map UserProperty {KEY_PRIORITY} to Message Priority: {e}"
                    ))
                })
            })
            .transpose()?;

        let ttl = if let Some(value) = props.find_user_property(KEY_TTL) {
            Some(value.parse::<u32>().map_err(|e| {
                invalid_argument(format!(
                    "Failed to map UserProperty {KEY_TTL} to Message TTL: {e}"
                ))
            })?)
        } else {
            props
                .get(paho_mqtt::PropertyCode::MessageExpiryInterval)
                .and_then(|property| property.get_u32())
                .map(|seconds| seconds.saturating_mul(1000))
        };

        let permission_level = props
            .find_user_property(KEY_PERMISSION_LEVEL)
            .map(|value| {
                value.parse::<u32>().map_err(|e| {
                    invalid_argument(format!(
                        "Failed to map UserProperty {KEY_PERMISSION_LEVEL} to Permission Level: {e}"
                    ))
                })
            })
            .transpose()?;
        let commstatus = props
            .find_user_property(KEY_COMMSTATUS)
            .map(|value| {
                value
                    .parse::<i32>()
                    .map_err(|e| {
                        invalid_argument(format!(
                            "Failed to map UserProperty {KEY_COMMSTATUS} to CommStatus: {e}"
                        ))
                    })
                    .and_then(|value| {
                        UCode::try_from_i32(value).map_err(|e| {
                            invalid_argument(format!(
                                "Failed to map UserProperty {KEY_COMMSTATUS} to CommStatus: {e}"
                            ))
                        })
                    })
            })
            .transpose()?;
        let request_id = props
            .get_binary(paho_mqtt::PropertyCode::CorrelationData)
            .map(|value| UUID::try_from(value).map_err(|e| invalid_argument(e.to_string())))
            .transpose()?;
        let token = props.find_user_property(KEY_TOKEN);
        let traceparent = props.find_user_property(KEY_TRACEPARENT);

        let content_type_count = props.iter(paho_mqtt::PropertyCode::ContentType).count();
        if content_type_count > 1 {
            return Err(invalid_argument(
                "MQTT message contains duplicate Content Type properties",
            ));
        }
        let payload_encoding = if content_type_count == 0 {
            None
        } else {
            let value = props
                .get_string(paho_mqtt::PropertyCode::ContentType)
                .ok_or_else(|| invalid_argument("MQTT Content Type is not a string"))?;
            let id = value.parse::<u32>().map_err(|e| {
                invalid_argument(format!(
                    "Failed to map Content Type to payload-encoding identifier: {e}"
                ))
            })?;
            Some(PayloadEncoding::from_id(id).map_err(|e| {
                invalid_argument(format!(
                    "Failed to map Content Type to payload-encoding identifier: {e}"
                ))
            })?)
        };

        if message_type != UMessageType::Request && (token.is_some() || permission_level.is_some())
        {
            return Err(invalid_argument(
                "token and permission level are only valid for request messages",
            ));
        }
        if message_type != UMessageType::Response && (commstatus.is_some() || request_id.is_some())
        {
            return Err(invalid_argument(
                "communication status and request id are only valid for response messages",
            ));
        }

        let message = match message_type {
            UMessageType::Publish => {
                if sink.is_some() {
                    return Err(invalid_argument("publish messages must not contain a sink"));
                }
                let mut builder = UMessageBuilder::publish(source);
                configure_common_attributes(
                    &mut builder,
                    &id,
                    priority,
                    ttl,
                    traceparent.as_deref(),
                );
                finish_message(
                    &mut builder,
                    payload_encoding.map(|_| Bytes::new()),
                    payload_encoding,
                )
            }
            UMessageType::Notification => {
                let mut builder = UMessageBuilder::notification(
                    source,
                    sink.ok_or_else(|| missing_required_attribute("sink"))?,
                );
                configure_common_attributes(
                    &mut builder,
                    &id,
                    priority,
                    ttl,
                    traceparent.as_deref(),
                );
                finish_message(
                    &mut builder,
                    payload_encoding.map(|_| Bytes::new()),
                    payload_encoding,
                )
            }
            UMessageType::Request => {
                let mut builder = UMessageBuilder::request(
                    sink.ok_or_else(|| missing_required_attribute("sink"))?,
                    source,
                    ttl.ok_or_else(|| missing_required_attribute("ttl"))?,
                );
                configure_common_attributes(
                    &mut builder,
                    &id,
                    priority,
                    ttl,
                    traceparent.as_deref(),
                );
                if let Some(token) = token {
                    builder.with_token(token);
                }
                if let Some(permission_level) = permission_level {
                    builder.with_permission_level(permission_level);
                }
                finish_message(
                    &mut builder,
                    payload_encoding.map(|_| Bytes::new()),
                    payload_encoding,
                )
            }
            UMessageType::Response => {
                let mut builder = UMessageBuilder::response(
                    sink.ok_or_else(|| missing_required_attribute("sink"))?,
                    request_id.ok_or_else(|| missing_required_attribute("request id"))?,
                    source,
                );
                configure_common_attributes(
                    &mut builder,
                    &id,
                    priority,
                    ttl,
                    traceparent.as_deref(),
                );
                if let Some(commstatus) = commstatus {
                    builder.with_comm_status(commstatus);
                }
                finish_message(
                    &mut builder,
                    payload_encoding.map(|_| Bytes::new()),
                    payload_encoding,
                )
            }
        }?;

        let attributes = message.attributes().clone();
        UAttributesValidators::validator_for_attributes(&attributes)
            .validate(&attributes)
            .map_err(|e| invalid_argument(format!("Failed to map message attributes: {e:?}")))?;
        attributes
            .check_expired()
            .map_err(|_| UStatus::fail_with_code(UCode::DeadlineExceeded, "message has expired"))?;
        Ok(attributes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const EVENT_SOURCE: &str = "//vin.vehicles/A8000/2/8A50";
    const REPLY_TO: &str = "//vin.vehicles/A8000/2/0";
    const METHOD: &str = "//vin.vehicles/B8000/3/1B50";
    const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    fn build_test_message(
        message_type: UMessageType,
        payload_encoding: Option<PayloadEncoding>,
    ) -> UMessage {
        let event_source = UUri::from_str(EVENT_SOURCE).expect("event source URI");
        let reply_to = UUri::from_str(REPLY_TO).expect("reply-to URI");
        let method = UUri::from_str(METHOD).expect("method URI");
        let id = UUID::build();
        match message_type {
            UMessageType::Publish => {
                let mut builder = UMessageBuilder::publish(event_source);
                builder
                    .with_message_id(id)
                    .with_priority(UPriority::CS5)
                    .with_traceparent(TRACEPARENT);
                finish_message(
                    &mut builder,
                    payload_encoding.map(|_| Bytes::from_static(b"payload")),
                    payload_encoding,
                )
                .expect("publish")
            }
            UMessageType::Notification => {
                let mut builder = UMessageBuilder::notification(event_source, reply_to);
                builder.with_message_id(id).with_ttl(2000);
                finish_message(
                    &mut builder,
                    payload_encoding.map(|_| Bytes::from_static(b"payload")),
                    payload_encoding,
                )
                .expect("notification")
            }
            UMessageType::Request => {
                let mut builder = UMessageBuilder::request(method, reply_to, 5400);
                builder
                    .with_message_id(id)
                    .with_permission_level(15)
                    .with_token("token")
                    .with_traceparent(TRACEPARENT);
                finish_message(
                    &mut builder,
                    payload_encoding.map(|_| Bytes::from_static(b"payload")),
                    payload_encoding,
                )
                .expect("request")
            }
            UMessageType::Response => {
                let mut builder = UMessageBuilder::response(reply_to, UUID::build(), method);
                builder
                    .with_message_id(id)
                    .with_comm_status(UCode::Unimplemented)
                    .with_traceparent(TRACEPARENT);
                finish_message(
                    &mut builder,
                    payload_encoding.map(|_| Bytes::from_static(b"payload")),
                    payload_encoding,
                )
                .expect("response")
            }
        }
    }

    fn minimal_publish_properties(id: &UUID) -> paho_mqtt::Properties {
        let mut properties = paho_mqtt::Properties::new();
        add_user_property(&mut properties, KEY_UPROTOCOL_VERSION, "1", "version").unwrap();
        add_user_property(
            &mut properties,
            KEY_MESSAGE_ID,
            &id.to_hyphenated_string(),
            "id",
        )
        .unwrap();
        add_user_property(
            &mut properties,
            KEY_TYPE,
            &UMessageType::Publish.to_cloudevent_type(),
            "type",
        )
        .unwrap();
        add_user_property(&mut properties, KEY_SOURCE, EVENT_SOURCE, "source").unwrap();
        properties
    }

    #[test_case(UMessageType::Publish, None; "publish without payload")]
    #[test_case(UMessageType::Notification, Some(PayloadEncoding::TEXT); "notification text")]
    #[test_case(UMessageType::Request, Some(PayloadEncoding::RAW); "request raw")]
    #[test_case(UMessageType::Response, Some(PayloadEncoding::JSON); "response json")]
    #[test_case(
        UMessageType::Publish,
        Some(PayloadEncoding::from_registry_entry(0x1000_0042));
        "publish private use"
    )]
    fn attributes_round_trip(
        message_type: UMessageType,
        payload_encoding: Option<PayloadEncoding>,
    ) {
        let message = build_test_message(message_type, payload_encoding);
        let mapper = DefaultMessageMapper;
        let properties = mapper
            .create_mqtt_properties_from_uattributes(message.attributes())
            .expect("MQTT properties");
        assert_eq!(
            properties.get_string(paho_mqtt::PropertyCode::ContentType),
            payload_encoding.map(|encoding| encoding.id().to_string())
        );
        let mapped = mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .expect("uAttributes");
        assert_eq!(&mapped, message.attributes());
    }

    #[test_case(""; "empty")]
    #[test_case("text/plain"; "media type")]
    #[test_case("-1"; "negative")]
    #[test_case("0"; "reserved zero")]
    #[test_case("4294967296"; "u32 overflow")]
    fn malformed_payload_encoding_is_rejected(value: &str) {
        let mapper = DefaultMessageMapper;
        let mut properties = minimal_publish_properties(&UUID::build());
        properties
            .push_string(paho_mqtt::PropertyCode::ContentType, value)
            .unwrap();
        assert!(mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .is_err_and(|error| error.code() == UCode::InvalidArgument));
    }

    #[test]
    fn duplicate_payload_encoding_is_rejected() {
        let mapper = DefaultMessageMapper;
        let mut properties = minimal_publish_properties(&UUID::build());
        properties
            .push_string(paho_mqtt::PropertyCode::ContentType, "7")
            .unwrap();
        properties
            .push_string(paho_mqtt::PropertyCode::ContentType, "6")
            .unwrap();
        assert!(mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .is_err_and(|error| error.code() == UCode::InvalidArgument));
    }

    #[test_case("13")]
    #[test_case("14")]
    #[test_case("15")]
    fn retired_payload_metadata_is_rejected(key: &str) {
        let mapper = DefaultMessageMapper;
        let mut properties = minimal_publish_properties(&UUID::build());
        add_user_property(&mut properties, key, "legacy", "legacy").unwrap();
        assert!(mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .is_err_and(|error| error.code() == UCode::InvalidArgument));
    }

    #[test]
    fn arbitrary_nonzero_registry_id_round_trips() {
        let mapper = DefaultMessageMapper;
        let mut properties = minimal_publish_properties(&UUID::build());
        properties
            .push_string(paho_mqtt::PropertyCode::ContentType, "66")
            .unwrap();
        let attributes = mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .expect("unknown nonzero identifiers remain representable");
        assert_eq!(
            attributes.payload_encoding(),
            Some(PayloadEncoding::from_registry_entry(66))
        );
    }

    #[test]
    fn permanently_assigned_registry_ids_round_trip() {
        let mapper = DefaultMessageMapper;
        for encoding in [
            PayloadEncoding::PROTOBUF_WRAPPED_IN_ANY,
            PayloadEncoding::PROTOBUF,
            PayloadEncoding::JSON,
            PayloadEncoding::SOMEIP,
            PayloadEncoding::SOMEIP_TLV,
            PayloadEncoding::RAW,
            PayloadEncoding::TEXT,
            PayloadEncoding::SHM,
        ] {
            let mut properties = minimal_publish_properties(&UUID::build());
            properties
                .push_string(
                    paho_mqtt::PropertyCode::ContentType,
                    &encoding.id().to_string(),
                )
                .unwrap();
            let attributes = mapper
                .create_uattributes_from_mqtt_properties(&properties)
                .expect("registered payload encoding");
            assert_eq!(attributes.payload_encoding(), Some(encoding));
            let remapped = mapper
                .create_mqtt_properties_from_uattributes(&attributes)
                .expect("remapped properties");
            assert_eq!(
                remapped.get_string(paho_mqtt::PropertyCode::ContentType),
                Some(encoding.id().to_string())
            );
        }
    }

    #[test]
    fn payload_presence_matches_encoding_presence() {
        let encoded = build_test_message(UMessageType::Publish, Some(PayloadEncoding::TEXT));
        let empty = create_umessage_from_uattributes(encoded.attributes(), Some(Bytes::new()))
            .expect("present empty payload");
        assert_eq!(empty.payload(), Some(Bytes::new()));
        assert_eq!(empty.payload_encoding(), Some(PayloadEncoding::TEXT));
        assert!(create_umessage_from_uattributes(encoded.attributes(), None).is_err());

        let unencoded = build_test_message(UMessageType::Publish, None);
        assert!(create_umessage_from_uattributes(unencoded.attributes(), None).is_ok());
        assert!(create_umessage_from_uattributes(
            unencoded.attributes(),
            Some(Bytes::from_static(b"payload"))
        )
        .is_err());
    }

    #[test]
    fn unsupported_uprotocol_version_is_rejected() {
        let mapper = DefaultMessageMapper;
        let mut properties = paho_mqtt::Properties::new();
        add_user_property(&mut properties, KEY_UPROTOCOL_VERSION, "2", "version").unwrap();
        assert!(mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .is_err_and(|error| error.code() == UCode::InvalidArgument));
    }

    #[test]
    fn invalid_source_is_rejected() {
        let mapper = DefaultMessageMapper;
        let mut properties = paho_mqtt::Properties::new();
        add_user_property(&mut properties, KEY_UPROTOCOL_VERSION, "1", "version").unwrap();
        add_user_property(
            &mut properties,
            KEY_MESSAGE_ID,
            &UUID::build().to_hyphenated_string(),
            "id",
        )
        .unwrap();
        add_user_property(
            &mut properties,
            KEY_TYPE,
            &UMessageType::Publish.to_cloudevent_type(),
            "type",
        )
        .unwrap();
        add_user_property(&mut properties, KEY_SOURCE, "not a URI", "source").unwrap();
        assert!(mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .is_err_and(|error| error.code() == UCode::InvalidArgument));
    }

    #[test]
    fn expired_message_is_rejected() {
        let mapper = DefaultMessageMapper;
        let expired_id = UUID::from_u64_pair(0x0000_0000_1000_7000, 0x8010_1010_1010_1a1a).unwrap();
        let mut properties = minimal_publish_properties(&expired_id);
        add_user_property(&mut properties, KEY_TTL, "12500", "ttl").unwrap();
        assert!(mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .is_err_and(|error| error.code() == UCode::DeadlineExceeded));
    }
}
