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
    PayloadEncoding, UAttributes, UAttributesValidators, UCode, UMessage, UMessageBuilder,
    UMessageType, UPayloadFormat, UPriority, UStatus, UUri, UUID,
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
const KEY_PAYLOAD_ENCODING_REGISTRY_ID: &str = "13";
const KEY_PAYLOAD_ENCODING: &str = "14";
const KEY_PAYLOAD_CONTENT_TYPE: &str = "15";

fn missing_required_attribute(name: &str) -> UStatus {
    UStatus::fail_with_code(
        UCode::InvalidArgument,
        format!("message attributes missing required {name}"),
    )
}

pub(crate) fn create_umessage_from_uattributes(
    attributes: &UAttributes,
    payload: Option<Bytes>,
) -> Result<UMessage, UStatus> {
    let mut builder = match attributes.type_() {
        UMessageType::Publish => UMessageBuilder::publish(attributes.source().clone()),
        UMessageType::Notification => UMessageBuilder::notification(
            attributes.source().clone(),
            attributes
                .sink()
                .ok_or_else(|| missing_required_attribute("sink"))?
                .clone(),
        ),
        UMessageType::Request => UMessageBuilder::request(
            attributes
                .sink()
                .ok_or_else(|| missing_required_attribute("sink"))?
                .clone(),
            attributes.source().clone(),
            attributes
                .ttl()
                .ok_or_else(|| missing_required_attribute("ttl"))?,
        ),
        UMessageType::Response => UMessageBuilder::response(
            attributes
                .sink()
                .ok_or_else(|| missing_required_attribute("sink"))?
                .clone(),
            attributes
                .request_id()
                .ok_or_else(|| missing_required_attribute("request id"))?
                .clone(),
            attributes.source().clone(),
        ),
    };

    builder.with_message_id(attributes.id().clone());

    if let Some(priority) = attributes.priority() {
        builder.with_priority(priority);
    }

    if let Some(ttl) = attributes.ttl() {
        builder.with_ttl(ttl);
    }

    if let Some(traceparent) = attributes.traceparent() {
        builder.with_traceparent(traceparent);
    }

    match attributes.type_() {
        UMessageType::Request => {
            if let Some(token) = attributes.token() {
                builder.with_token(token);
            }
            if let Some(permission_level) = attributes.permission_level() {
                builder.with_permission_level(permission_level);
            }
        }
        UMessageType::Response => {
            if let Some(commstatus) = attributes.commstatus() {
                builder.with_comm_status(commstatus);
            }
        }
        UMessageType::Publish | UMessageType::Notification => {}
    }

    let payload_format = attributes
        .payload_format()
        .unwrap_or(UPayloadFormat::Unspecified);
    let open_payload_encoding = attributes.open_payload_encoding_parts();

    let result = if let Some(payload) = payload {
        if open_payload_encoding != (None, None, None) {
            builder.build_with_payload_encoding(
                payload,
                open_payload_encoding_from_parts(
                    open_payload_encoding.0,
                    open_payload_encoding.1,
                    open_payload_encoding.2,
                )?,
            )
        } else {
            builder.build_with_payload(payload, payload_format)
        }
    } else {
        builder.build()
    };

    result.map_err(|e| UStatus::fail_with_code(UCode::InvalidArgument, e.to_string()))
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

fn open_payload_encoding_from_parts(
    registry_id: Option<u32>,
    encoding: Option<&str>,
    content_type: Option<&str>,
) -> Result<PayloadEncoding, UStatus> {
    PayloadEncoding::from_parts(
        registry_id,
        encoding.map(ToOwned::to_owned),
        content_type.map(ToOwned::to_owned),
    )
    .map_err(|err| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            format!("Failed to map open payload encoding fields: {err}"),
        )
    })
}

#[cfg_attr(test, mockall::automock)]
pub(crate) trait MessageMapper: Send + Sync {
    /// Creates MQTT 5 header properties from uProtocol message meta data
    /// as defined by uProtocol's MQTT5 transport specification.
    ///
    /// # Arguments
    /// * `attributes` - The message meta data.
    ///
    /// # Errors
    ///
    /// Returns an error if the given meta data are invalid or cannot
    /// be mapped to MQTT properties.
    fn create_mqtt_properties_from_uattributes(
        &self,
        attributes: &UAttributes,
    ) -> Result<paho_mqtt::Properties, UStatus>;

    /// Creates uProtocol message meta data from MQTT header properties.
    ///
    /// # Arguments
    /// * `props` - MQTT properties to get meta data from.
    ///
    /// # Errors
    ///
    /// Returns an error if the MQTT header properties cannot be mapped to valid uProtocol message meta data.
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
        // No need to start conversion if attributes are invalid
        // [impl->dsn~utransport-send-error-invalid-parameter~1]
        UAttributesValidators::get_validator_for_attributes(attributes)
            .validate(attributes)
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("Invalid uAttributes, err: {e:?}"),
                )
            })?;

        let mut properties = paho_mqtt::Properties::new();

        // Add uProtocol major version
        add_user_property(
            &mut properties,
            KEY_UPROTOCOL_VERSION,
            CURRENT_UPROTOCOL_MAJOR_VERSION.to_string().as_str(),
            "Failed to add uProtocol major version to MQTT User Properties",
        )?;

        // Add TTL
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
                // TTL does not have full second granularity so we need to also add
                // a dedicated user property to be able to recreate the original
                // value at the receiving end
                add_user_property(
                    &mut properties,
                    KEY_TTL,
                    ttl.to_string().as_str(),
                    "Failed to add TTL to MQTT User Properties",
                )?;
            }
        }

        add_user_property(
            &mut properties,
            KEY_MESSAGE_ID,
            &attributes.id().to_hyphenated_string(),
            "Failed to add message ID to mqtt User Properties",
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

        if let Some(prio) = attributes.priority() {
            add_user_property(
                &mut properties,
                KEY_PRIORITY,
                prio.to_priority_code(),
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

        if let Some(comm_status) = attributes.commstatus() {
            add_user_property(
                &mut properties,
                KEY_COMMSTATUS,
                &comm_status.value().to_string(),
                "Failed to add message comm status to MQTT User Properties",
            )?;
        }

        if let Some(req_id) = attributes.request_id() {
            properties
                .push_binary::<Vec<u8>>(paho_mqtt::PropertyCode::CorrelationData, req_id.into())
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

        if let Some(format) = attributes.payload_format() {
            if format != UPayloadFormat::Unspecified {
                properties
                    .push_string(
                        paho_mqtt::PropertyCode::ContentType,
                        &format.as_i32().to_string(),
                    )
                    .map_err(|e| {
                        UStatus::fail_with_code(
                            UCode::Internal,
                            format!("Failed to create Content Type property: {e:?}"),
                        )
                    })?;
            }
        }

        let (payload_encoding_registry_id, payload_encoding, payload_content_type) =
            attributes.open_payload_encoding_parts();
        if let Some(registry_id) = payload_encoding_registry_id {
            add_user_property(
                &mut properties,
                KEY_PAYLOAD_ENCODING_REGISTRY_ID,
                &registry_id.to_string(),
                "Failed to add payload encoding registry ID to MQTT User Properties",
            )?;
        }
        if let Some(encoding) = payload_encoding {
            add_user_property(
                &mut properties,
                KEY_PAYLOAD_ENCODING,
                encoding,
                "Failed to add payload encoding to MQTT User Properties",
            )?;
        }
        if let Some(content_type) = payload_content_type {
            add_user_property(
                &mut properties,
                KEY_PAYLOAD_CONTENT_TYPE,
                content_type,
                "Failed to add payload content type to MQTT User Properties",
            )?;
        }

        Ok(properties)
    }

    // [impl->dsn~up-transport-mqtt5-attributes-mapping~1]
    fn create_uattributes_from_mqtt_properties(
        &self,
        props: &paho_mqtt::Properties,
    ) -> Result<UAttributes, UStatus> {
        let uprotocol_major_version = props.find_user_property(KEY_UPROTOCOL_VERSION)
        .ok_or_else(||UStatus::fail_with_code(UCode::InvalidArgument, "MQTT message does not contain uProtocol version identifier"))
        .and_then(|s| s.parse::<u8>().map_err(|err| UStatus::fail_with_code(UCode::InvalidArgument, format!("Failed to map UserProperty {KEY_UPROTOCOL_VERSION} to uProtocol major version: {err}"))))?;
        if uprotocol_major_version != CURRENT_UPROTOCOL_MAJOR_VERSION {
            return Err(UStatus::fail_with_code(UCode::InvalidArgument, format!("MQTT message contains unsupported uProtocol major version [expected: {CURRENT_UPROTOCOL_MAJOR_VERSION}, found: {uprotocol_major_version}")));
        }

        let token = props.find_user_property(KEY_TOKEN);
        let traceparent = props.find_user_property(KEY_TRACEPARENT);

        let id = if let Some(message_id) = props.find_user_property(KEY_MESSAGE_ID) {
            let id = UUID::from_str(&message_id).map_err(|e| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("Failed to map UserProperty {KEY_MESSAGE_ID} to Message ID: {e}"),
                )
            })?;
            Some(id)
        } else {
            None
        };

        let message_type = if let Some(message_type_str) = props.find_user_property(KEY_TYPE) {
            UMessageType::try_from_cloudevent_type(message_type_str).map_err(|e| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("Failed to map UserProperty {KEY_TYPE} to Message Type: {e}"),
                )
            })?
        } else {
            return Err(missing_required_attribute("message type"));
        };

        let source = if let Some(source_string) = props.find_user_property(KEY_SOURCE) {
            UUri::from_str(&source_string).map_err(|err| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("Failed to map UserProperty {KEY_SOURCE} to Message Source: {err}"),
                )
            })?
        } else {
            return Err(missing_required_attribute("source"));
        };

        let sink = if let Some(sink_string) = props.find_user_property(KEY_SINK) {
            Some(UUri::from_str(&sink_string).map_err(|err| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("Failed to map UserProperty {KEY_SINK} to Message Sink: {err}"),
                )
            })?)
        } else {
            None
        };

        let priority =
            if let Some(priority_string) = props.find_user_property(KEY_PRIORITY) {
                Some(
                    UPriority::try_from_priority_code(priority_string).map_err(|e| {
                        UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("Failed to map UserProperty {KEY_PRIORITY} to Message Priority: {e}"),
                )
                    })?,
                )
            } else {
                // [impl->dsn~up-attributes-priority~1]
                // it is sufficient to set to UNSPECIFIED because according to the spec,
                // a message without a (concrete) priority, belongs to class CS1 by default
                None
            };

        let ttl = if let Some(ttl_string) = props.find_user_property(KEY_TTL) {
            // Add the TTL UAttribute from TTL user property if it is set
            Some(ttl_string.parse::<u32>().map_err(|e| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("Failed to map UserProperty {KEY_TTL} to Message TTL: {e}"),
                )
            })?)
        } else if let Some(message_expiry_interval) = props
            .get(paho_mqtt::PropertyCode::MessageExpiryInterval)
            .and_then(|prop| prop.get_u32())
        {
            // otherwise, fall back to the MessageExpiryInterval if available
            message_expiry_interval.checked_mul(1000).or(Some(u32::MAX))
        } else {
            None
        };

        let permission_level = if let Some(permission_string) =
            props.find_user_property(KEY_PERMISSION_LEVEL)
        {
            permission_string
            .parse()
            .map_err(|err| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!(
                        "Failed to map UserProperty {KEY_PERMISSION_LEVEL} to Permission Level: {err}"
                    ),
                )
            })
            .map(Option::Some)?
        } else {
            None
        };

        let commstatus = if let Some(comm_status_string) = props.find_user_property(KEY_COMMSTATUS)
        {
            comm_status_string
            .parse::<i32>()
            .map_err(|err| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("Failed to map UserProperty {KEY_COMMSTATUS} to CommStatus: {err}"),
                )
            })
            .and_then(|v| {
                UCode::try_from_i32(v).map_err(|_|{
                    UStatus::fail_with_code(
                        UCode::InvalidArgument,
                        format!("Failed to map UserProperty {KEY_COMMSTATUS} to CommStatus: not a valid UCode [{v}]"),
                    )
                })
            })
            .map(Option::Some)?
        } else {
            None
        };

        let reqid = if let Some(req_id) = props.get_binary(paho_mqtt::PropertyCode::CorrelationData)
        {
            Some(
                UUID::try_from(req_id)
                    .map_err(|e| UStatus::fail_with_code(UCode::InvalidArgument, e.to_string()))?,
            )
        } else {
            None
        };

        let payload_format = if let Some(payload_format_string) =
            props.get_string(paho_mqtt::PropertyCode::ContentType)
        {
            Some(payload_format_string
            .parse::<i32>()
            .map_err(|err| UStatus::fail_with_code(
              UCode::InvalidArgument,
              format!("Failed to map Content Type to Message Payload Format: {err}")))
            .and_then(|v| {
                UPayloadFormat::try_from_i32(v).map_err(|_|UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("Failed to map Content Type to Message Payload Format: not a valid payload format code [{v}]"),
                ))
            })?)
        } else {
            None
        };
        let open_payload_encoding_registry_id = props
            .find_user_property(KEY_PAYLOAD_ENCODING_REGISTRY_ID)
            .map(|value| {
                value.parse::<u32>().map_err(|err| {
                    UStatus::fail_with_code(
                        UCode::InvalidArgument,
                        format!(
                            "Failed to map UserProperty {KEY_PAYLOAD_ENCODING_REGISTRY_ID} to Payload Encoding Registry ID: {err}"
                        ),
                    )
                })
            })
            .transpose()?;
        let open_payload_encoding = props.find_user_property(KEY_PAYLOAD_ENCODING);
        let open_payload_content_type = props.find_user_property(KEY_PAYLOAD_CONTENT_TYPE);
        let open_payload_encoding_present = open_payload_encoding_registry_id.is_some()
            || open_payload_encoding.is_some()
            || open_payload_content_type.is_some();
        if payload_format.is_some_and(|format| format != UPayloadFormat::Unspecified)
            && open_payload_encoding_present
        {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "MQTT message contains both Content Type payload_format and open payload encoding user properties",
            ));
        }

        let mut builder = match message_type {
            UMessageType::Publish => UMessageBuilder::publish(source),
            UMessageType::Notification => UMessageBuilder::notification(
                source,
                sink.ok_or_else(|| missing_required_attribute("sink"))?,
            ),
            UMessageType::Request => UMessageBuilder::request(
                sink.ok_or_else(|| missing_required_attribute("sink"))?,
                source,
                ttl.ok_or_else(|| missing_required_attribute("ttl"))?,
            ),
            UMessageType::Response => UMessageBuilder::response(
                sink.ok_or_else(|| missing_required_attribute("sink"))?,
                reqid.ok_or_else(|| missing_required_attribute("request id"))?,
                source,
            ),
        };

        if let Some(id) = id {
            builder.with_message_id(id);
        }
        if let Some(priority) = priority {
            builder.with_priority(priority);
        }
        if let Some(ttl) = ttl {
            builder.with_ttl(ttl);
        }
        if let Some(traceparent) = traceparent {
            builder.with_traceparent(traceparent);
        }
        match message_type {
            UMessageType::Request => {
                if let Some(token) = token {
                    builder.with_token(token);
                }
                if let Some(permission_level) = permission_level {
                    builder.with_permission_level(permission_level);
                }
            }
            UMessageType::Response => {
                if let Some(commstatus) = commstatus {
                    builder.with_comm_status(commstatus);
                }
            }
            UMessageType::Publish | UMessageType::Notification => {}
        }

        let message = if open_payload_encoding_present {
            let payload_encoding = open_payload_encoding_from_parts(
                open_payload_encoding_registry_id,
                open_payload_encoding.as_deref(),
                open_payload_content_type.as_deref(),
            )?;
            if payload_encoding.to_legacy_format().is_some() {
                return Err(UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    "registered payload encodings must use MQTT Content Type payload_format, not open payload encoding user properties",
                ));
            }
            builder.build_with_payload_encoding(Bytes::new(), payload_encoding)
        } else {
            builder.build_with_payload(
                Bytes::new(),
                payload_format.unwrap_or(UPayloadFormat::Unspecified),
            )
        };
        let attributes = message
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("Failed to map message attributes: {e}"),
                )
            })?
            .attributes()
            .clone();

        // Validate the reconstructed attributes
        let validator = UAttributesValidators::get_validator_for_attributes(&attributes);
        validator.validate(&attributes).map_err(|e| {
            UStatus::fail_with_code(
                UCode::InvalidArgument,
                format!("Failed to map message attributes: {e:?}"),
            )
        })?;

        // [impl->dsn~up-attributes-ttl~1]
        // [impl->dsn~up-attributes-ttl-timeout~1]
        attributes.check_expired().map_err(|_err| {
            UStatus::fail_with_code(UCode::DeadlineExceeded, "message has expired")
        })?;

        Ok(attributes)
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    const MSG_TOKEN: &str = "the token";
    const MSG_TRACEPARENT: &str = "traceparent";
    const MSG_PERMISSION_LEVEL: u32 = 15;

    // Helper function used to create a UAttributes object and corresponding
    // MQTT properties object for testing and comparison
    #[allow(clippy::too_many_arguments)]
    fn create_test_uattributes_and_properties(
        major_version: Option<u8>,
        type_: Option<UMessageType>,
        id: Option<&UUID>,
        source: Option<&str>,
        sink: Option<&str>,
        priority: Option<UPriority>,
        ttl: Option<u32>, // milliseconds
        perm_level: Option<u32>,
        commstatus: Option<UCode>,
        reqid: Option<&UUID>,
        token: Option<&str>,
        traceparent: Option<&str>,
        payload_format: Option<UPayloadFormat>,
    ) -> (UAttributes, paho_mqtt::Properties) {
        let uattributes = create_uattributes(
            type_,
            id,
            source,
            sink,
            priority,
            ttl,
            perm_level,
            commstatus,
            reqid,
            token,
            traceparent,
            payload_format,
        );

        let properties = create_mqtt_properties(
            major_version,
            type_,
            id,
            source,
            sink,
            priority,
            ttl,
            perm_level,
            commstatus,
            reqid,
            token,
            traceparent,
            payload_format,
        );

        (uattributes, properties)
    }

    // Helper function to construct UAttributes object for testing.
    #[allow(clippy::too_many_arguments)]
    fn create_uattributes(
        type_: Option<UMessageType>,
        id: Option<&UUID>,
        source: Option<&str>,
        sink: Option<&str>,
        priority: Option<UPriority>,
        ttl: Option<u32>, // milliseconds
        permission_level: Option<u32>,
        commstatus: Option<UCode>,
        reqid: Option<&UUID>,
        token: Option<&str>,
        traceparent: Option<&str>,
        payload_format: Option<UPayloadFormat>,
    ) -> UAttributes {
        let message_type = type_.expect("expected message type");
        let source =
            UUri::from_str(source.expect("expected source")).expect("expected valid source URI");
        let sink = sink.map(|uri| UUri::from_str(uri).expect("expected valid sink URI"));

        let mut builder = match message_type {
            UMessageType::Publish => UMessageBuilder::publish(source),
            UMessageType::Notification => {
                UMessageBuilder::notification(source, sink.expect("expected sink"))
            }
            UMessageType::Request => UMessageBuilder::request(
                sink.expect("expected sink"),
                source,
                ttl.expect("expected ttl"),
            ),
            UMessageType::Response => UMessageBuilder::response(
                sink.expect("expected sink"),
                reqid.cloned().expect("expected request ID"),
                source,
            ),
        };

        if let Some(id) = id {
            builder.with_message_id(id.clone());
        }
        if let Some(priority) = priority {
            builder.with_priority(priority);
        }
        if let Some(ttl) = ttl {
            builder.with_ttl(ttl);
        }
        if let Some(traceparent) = traceparent {
            builder.with_traceparent(traceparent);
        }
        match message_type {
            UMessageType::Request => {
                if let Some(token) = token {
                    builder.with_token(token);
                }
                if let Some(permission_level) = permission_level {
                    builder.with_permission_level(permission_level);
                }
            }
            UMessageType::Response => {
                if let Some(commstatus) = commstatus {
                    builder.with_comm_status(commstatus);
                }
            }
            UMessageType::Publish | UMessageType::Notification => {}
        }

        builder
            .build_with_payload(
                Bytes::new(),
                payload_format.unwrap_or(UPayloadFormat::Unspecified),
            )
            .expect("expected valid test message")
            .attributes()
            .clone()
    }

    // Helper function to create mqtt properties for testing.
    #[allow(clippy::too_many_arguments)]
    fn create_mqtt_properties(
        major_version: Option<u8>,
        type_: Option<UMessageType>,
        id: Option<&UUID>,
        source: Option<&str>,
        sink: Option<&str>,
        priority: Option<UPriority>,
        ttl: Option<u32>, // milliseconds
        perm_level: Option<u32>,
        commstatus: Option<UCode>,
        reqid: Option<&UUID>,
        token: Option<&str>,
        traceparent: Option<&str>,
        payload_format: Option<UPayloadFormat>,
    ) -> paho_mqtt::Properties {
        let mut properties = paho_mqtt::Properties::new();

        if let Some(version) = major_version {
            properties
                .push_string_pair(
                    paho_mqtt::PropertyCode::UserProperty,
                    KEY_UPROTOCOL_VERSION,
                    version.to_string().as_str(),
                )
                .unwrap();
        }

        if let Some(type_val) = type_ {
            properties
                .push_string_pair(
                    paho_mqtt::PropertyCode::UserProperty,
                    KEY_TYPE,
                    &type_val.to_cloudevent_type(),
                )
                .unwrap();
        }

        if let Some(id_val) = id {
            properties
                .push_string_pair(
                    paho_mqtt::PropertyCode::UserProperty,
                    KEY_MESSAGE_ID,
                    &id_val.to_hyphenated_string(),
                )
                .unwrap();
        }

        if let Some(source_val) = source {
            properties
                .push_string_pair(
                    paho_mqtt::PropertyCode::UserProperty,
                    KEY_SOURCE,
                    source_val,
                )
                .unwrap();
        }
        if let Some(sink_val) = sink {
            properties
                .push_string_pair(paho_mqtt::PropertyCode::UserProperty, KEY_SINK, sink_val)
                .unwrap();
        }
        if let Some(priority_val) = priority.filter(|v| *v != UPriority::CS1) {
            properties
                .push_string_pair(
                    paho_mqtt::PropertyCode::UserProperty,
                    KEY_PRIORITY,
                    priority_val.to_priority_code(),
                )
                .unwrap();
        }
        if let Some(v) = ttl {
            properties
                .push_u32(
                    paho_mqtt::PropertyCode::MessageExpiryInterval,
                    v.div_ceil(1000),
                )
                .unwrap();
            if v % 1000 > 0 {
                properties
                    .push_string_pair(
                        paho_mqtt::PropertyCode::UserProperty,
                        KEY_TTL,
                        &v.to_string(),
                    )
                    .unwrap();
            }
        }
        if let Some(perm_level_val) = perm_level {
            properties
                .push_string_pair(
                    paho_mqtt::PropertyCode::UserProperty,
                    KEY_PERMISSION_LEVEL,
                    &perm_level_val.to_string(),
                )
                .unwrap();
        }
        if let Some(commstatus_val) = commstatus {
            properties
                .push_string_pair(
                    paho_mqtt::PropertyCode::UserProperty,
                    KEY_COMMSTATUS,
                    &commstatus_val.value().to_string(),
                )
                .unwrap();
        }
        if let Some(reqid_val) = reqid {
            properties
                .push_binary::<Vec<u8>>(paho_mqtt::PropertyCode::CorrelationData, reqid_val.into())
                .inspect_err(|err| println!("{err}"))
                .expect("Failed to set Correlation Data property");
        }
        if let Some(token_val) = token {
            properties
                .push_string_pair(paho_mqtt::PropertyCode::UserProperty, KEY_TOKEN, token_val)
                .unwrap();
        }
        if let Some(traceparent_val) = traceparent {
            properties
                .push_string_pair(
                    paho_mqtt::PropertyCode::UserProperty,
                    KEY_TRACEPARENT,
                    traceparent_val,
                )
                .unwrap();
        }
        if let Some(payload_format_val) = payload_format {
            properties
                .push_string(
                    paho_mqtt::PropertyCode::ContentType,
                    &payload_format_val.as_i32().to_string(),
                )
                .unwrap();
        }

        properties
    }

    fn open_payload_encoding() -> PayloadEncoding {
        PayloadEncoding::custom("up.xcdr-v2", "application/vnd.uprotocol.xcdr-v2")
            .expect("open payload encoding")
    }

    fn open_payload_encoding_attributes() -> UAttributes {
        UMessageBuilder::publish(UUri::from_str("/A10D/4/B3AA").expect("valid source"))
            .build_with_payload_encoding(Bytes::new(), open_payload_encoding())
            .expect("valid message")
            .attributes()
            .clone()
    }

    /// Verifies that two sets of MQTT properties contain the same items.
    fn assert_mqtt_properties(
        properties: &paho_mqtt::Properties,
        expected_properties: &paho_mqtt::Properties,
    ) {
        assert_eq!(
            properties.get_string(paho_mqtt::PropertyCode::ContentType),
            expected_properties.get_string(paho_mqtt::PropertyCode::ContentType)
        );
        assert_eq!(
            properties
                .get(paho_mqtt::PropertyCode::MessageExpiryInterval)
                .map(|prop| prop.get_u32()),
            expected_properties
                .get(paho_mqtt::PropertyCode::MessageExpiryInterval)
                .map(|prop| prop.get_u32())
        );
        properties.user_iter().for_each(|(key, value)| {
            assert_eq!(expected_properties.find_user_property(&key), Some(value));
        });
    }

    #[test]
    fn mqtt_properties_include_open_payload_encoding_user_properties() {
        let mapper = DefaultMessageMapper;
        let properties = mapper
            .create_mqtt_properties_from_uattributes(&open_payload_encoding_attributes())
            .expect("properties");

        assert_eq!(
            properties.get_string(paho_mqtt::PropertyCode::ContentType),
            None
        );
        assert_eq!(
            properties.find_user_property(KEY_PAYLOAD_ENCODING_REGISTRY_ID),
            None
        );
        assert_eq!(
            properties.find_user_property(KEY_PAYLOAD_ENCODING),
            Some("up.xcdr-v2".to_string())
        );
        assert_eq!(
            properties.find_user_property(KEY_PAYLOAD_CONTENT_TYPE),
            Some("application/vnd.uprotocol.xcdr-v2".to_string())
        );
    }

    #[test]
    fn mqtt_properties_decode_open_payload_encoding_user_properties() {
        let mapper = DefaultMessageMapper;
        let message_id = UUID::build();
        let mut properties = create_mqtt_properties(
            Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
            Some(UMessageType::Publish),
            Some(&message_id),
            Some("//vin.vehicles/A8000/2/8A50"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        properties
            .push_string_pair(
                paho_mqtt::PropertyCode::UserProperty,
                KEY_PAYLOAD_ENCODING,
                "up.xcdr-v2",
            )
            .unwrap();
        properties
            .push_string_pair(
                paho_mqtt::PropertyCode::UserProperty,
                KEY_PAYLOAD_CONTENT_TYPE,
                "application/vnd.uprotocol.xcdr-v2",
            )
            .unwrap();

        let attributes = mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .expect("attributes");

        assert!(matches!(
            attributes.payload_format(),
            None | Some(UPayloadFormat::Unspecified)
        ));
        assert_eq!(
            attributes.open_payload_encoding_parts(),
            (
                None,
                Some("up.xcdr-v2"),
                Some("application/vnd.uprotocol.xcdr-v2")
            )
        );
    }

    #[test]
    fn create_umessage_from_uattributes_preserves_open_payload_encoding() {
        let attributes = open_payload_encoding_attributes();
        let message = create_umessage_from_uattributes(
            &attributes,
            Some(Bytes::from_static(b"encoded payload")),
        )
        .expect("message");

        assert_eq!(
            message.payload(),
            Some(Bytes::from_static(b"encoded payload"))
        );
        assert_eq!(
            message.attributes().open_payload_encoding_parts(),
            attributes.open_payload_encoding_parts()
        );
    }

    #[test]
    fn mqtt_properties_reject_both_payload_encoding_mechanisms() {
        let mapper = DefaultMessageMapper;
        let message_id = UUID::build();
        let mut properties = create_mqtt_properties(
            Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
            Some(UMessageType::Publish),
            Some(&message_id),
            Some("//vin.vehicles/A8000/2/8A50"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(UPayloadFormat::Text),
        );
        properties
            .push_string_pair(
                paho_mqtt::PropertyCode::UserProperty,
                KEY_PAYLOAD_ENCODING,
                "up.xcdr-v2",
            )
            .unwrap();
        properties
            .push_string_pair(
                paho_mqtt::PropertyCode::UserProperty,
                KEY_PAYLOAD_CONTENT_TYPE,
                "application/vnd.uprotocol.xcdr-v2",
            )
            .unwrap();

        assert!(mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .is_err_and(|err| err.get_code() == UCode::InvalidArgument));
    }

    #[test]
    fn mqtt_properties_reject_registered_encoding_in_open_fields() {
        let mapper = DefaultMessageMapper;
        let message_id = UUID::build();
        let mut properties = create_mqtt_properties(
            Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
            Some(UMessageType::Publish),
            Some(&message_id),
            Some("//vin.vehicles/A8000/2/8A50"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        properties
            .push_string_pair(
                paho_mqtt::PropertyCode::UserProperty,
                KEY_PAYLOAD_ENCODING,
                "up.raw",
            )
            .unwrap();
        properties
            .push_string_pair(
                paho_mqtt::PropertyCode::UserProperty,
                KEY_PAYLOAD_CONTENT_TYPE,
                "application/octet-stream",
            )
            .unwrap();

        assert!(mapper
            .create_uattributes_from_mqtt_properties(&properties)
            .is_err_and(|err| err.get_code() == UCode::InvalidArgument));
    }

    //
    // MQTT Properties -> UAttributes
    //

    #[test_case(
        create_test_uattributes_and_properties(
            Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
            Some(UMessageType::Publish),
            Some(&UUID::build()),
            Some("//vin.vehicles/A8000/2/8A50"),
            None,
            Some(UPriority::CS5),
            None, None, None, None, None, None,
            Some(UPayloadFormat::Text),
        ),
        None;
        "for valid Publish message"
    )]
    // [utest->dsn~up-attributes-priority~1]
    // [utest->dsn~up-attributes-ttl~1]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
            Some(UMessageType::Notification),
            Some(&UUID::from_u64_pair(
                // timestamp: 1000ms since UNIX epoch
                0x0000000010007000_u64,
                0x8010101010101a1a_u64,
            ).unwrap()),
            Some("//vin.vehicles/A8000/2/1A50"),
            Some("//vin.vehicles/B8000/3/0"),
            None,
            // do not expire
            Some(0),
            None, None, None, None,
            Some(MSG_TRACEPARENT),
            None
        ),
        None;
        "for valid Notification"
    )]
    // [utest->dsn~up-attributes-ttl-timeout~1]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
            Some(UMessageType::Request),
            Some(&UUID::build()),
            Some("//vin.vehicles/A8000/2/0"),
            Some("//vin.vehicles/B8000/3/1B50"),
            Some(UPriority::CS4),
            Some(5400),
            Some(MSG_PERMISSION_LEVEL),
            None, None,
            Some(MSG_TOKEN),
            Some(MSG_TRACEPARENT),
            Some(UPayloadFormat::Raw)
        ),
        None;
        "for valid Request"
    )]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
            Some(UMessageType::Response),
            Some(&UUID::build()),
            Some("//vin.vehicles/B8000/3/1B50"),
            Some("//vin.vehicles/A8000/2/0"),
            Some(UPriority::CS4),
            Some(3000),
            None,
            Some(UCode::Unimplemented),
            Some(&UUID::build()),
            None,
            Some(MSG_TRACEPARENT),
            None
        ),
        None;
        "for valid Response"
    )]
    #[test_case(
        (
            create_uattributes(
                Some(UMessageType::Publish),
                Some(&UUID::build()),
                Some("//vin.vehicles/A8000/2/AA50"),
                None, None, None, None, None, None, None, None, None,
            ),
            create_mqtt_properties(
                Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
                Some(UMessageType::Publish),
                Some(&UUID::build()),
                // source must have resource ID >= 0x8000
                Some("//vin.vehicles/A8000/2/1A50"),
                None, None, None, None, None, None, None, None, None,
            )
        ),
        Some(UCode::InvalidArgument);
        "fails for Publish message with invalid source URI"
    )]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(CURRENT_UPROTOCOL_MAJOR_VERSION + 1),
            Some(UMessageType::Publish),
            Some(&UUID::build()),
            Some("//vin.vehicles/A8000/2/AA50"),
            None, None, None, None, None, None, None, None, None
        ),
        Some(UCode::InvalidArgument);
        "fails for Publish message with invalid uProtocol version"
    )]
    // [utest->dsn~up-attributes-ttl-timeout~1]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
            Some(UMessageType::Publish),
            Some(&UUID::from_u64_pair(
                // timestamp: 1000ms since UNIX epoch
                0x0000000010007000_u64,
                0x8010101010101a1a_u64,
            ).unwrap()),
            Some("//vin.vehicles/A8000/2/AA50"),
            None,
            None,
            // message expiry interval: 12.5s
            // i.e. this message has expired decades ago
            Some(12500),
            None, None, None, None, None, None
        ),
        Some(UCode::DeadlineExceeded);
        "fails for expired Publish message"
    )]
    // [utest->dsn~up-transport-mqtt5-attributes-mapping~1]
    fn test_create_uattributes_from_mqtt_properties(
        (expected_attributes, mqtt_properties): (UAttributes, paho_mqtt::Properties),
        expected_error_code: Option<UCode>,
    ) {
        let mapper = DefaultMessageMapper;
        let attributes_result = mapper.create_uattributes_from_mqtt_properties(&mqtt_properties);
        if let Some(code) = expected_error_code {
            assert!(attributes_result.is_err_and(|err| err.get_code() == code))
        } else {
            assert!(attributes_result.is_ok_and(|attribs| attribs == expected_attributes));
        }
    }

    //
    // UAttributes -> MQTT Properties
    //

    #[test_case(create_test_uattributes_and_properties(
        Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
        Some(UMessageType::Publish),
        Some(&UUID::build()),
        Some("/A10D/4/B3AA"),
        None,
        Some(UPriority::CS5),
        None,
        None,
        None,
        None,
        None,
        None,
        Some(UPayloadFormat::Text)
    );"for Publish message")]
    #[test_case(create_test_uattributes_and_properties(
        Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
        Some(UMessageType::Notification),
        Some(&UUID::build()),
        Some("/A10D/4/B3AA"),
        Some("/103/2/0"),
        None,
        None,
        None,
        None,
        None,
        Some(MSG_TRACEPARENT),
        None,
        None
    );"for Notification message")]
    #[test_case(create_test_uattributes_and_properties(
        Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
        Some(UMessageType::Request),
        Some(&UUID::build()),
        Some("/A10D/4/0"),
        Some("/103/2/71A3"),
        Some(UPriority::CS4),
        Some(2000),
        Some(MSG_PERMISSION_LEVEL),
        None,
        None,
        Some(MSG_TOKEN),
        Some(MSG_TRACEPARENT),
        Some(UPayloadFormat::Raw)
    );"for RPC Request message")]
    #[test_case(create_test_uattributes_and_properties(
        Some(CURRENT_UPROTOCOL_MAJOR_VERSION),
        Some(UMessageType::Response),
        Some(&UUID::build()),
        Some("/103/2/71A3"),
        Some("/A10D/4/0"),
        Some(UPriority::CS4),
        Some(4150),
        None,
        Some(UCode::Unimplemented),
        Some(&UUID::build()),
        None,
        Some(MSG_TRACEPARENT),
        None
    );"for RPC Response message")]
    // [utest->dsn~up-transport-mqtt5-attributes-mapping~1]
    fn test_create_mqtt_properties_from_uattributes(
        (attributes, expected_mqtt_properties): (UAttributes, paho_mqtt::Properties),
    ) {
        let mapper = DefaultMessageMapper;
        let mqtt_properties = mapper
            .create_mqtt_properties_from_uattributes(&attributes)
            .expect("failed to create MQTT properties from message attributes");
        assert_mqtt_properties(&mqtt_properties, &expected_mqtt_properties);
    }
}
