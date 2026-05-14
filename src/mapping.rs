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

use up_rust::{
    UAttributes, UCode, UEncoding, UFrameMetadata, UMessageType, UPriority, UStatus, UUri, UUID,
};

const CURRENT_UPROTOCOL_MAJOR_VERSION: u8 = 1;

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
const KEY_ENCODING_FORMAT_ID: &str = "uformatid";
const KEY_ENCODING_SCHEMA_REF: &str = "uschemaref";

fn add_user_property(
    properties: &mut paho_mqtt::Properties,
    key: &str,
    value: &str,
    error_message: &str,
) -> Result<(), UStatus> {
    properties
        .push_string_pair(paho_mqtt::PropertyCode::UserProperty, key, value)
        .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, format!("{error_message}: {e:?}")))
}

#[cfg_attr(test, mockall::automock)]
pub(crate) trait MessageMapper: Send + Sync {
    fn create_mqtt_properties_from_frame_metadata(
        &self,
        header: &UFrameMetadata,
    ) -> Result<paho_mqtt::Properties, UStatus>;

    fn create_frame_metadata_from_mqtt_properties(
        &self,
        props: &paho_mqtt::Properties,
    ) -> Result<UFrameMetadata, UStatus>;
}

#[derive(Default)]
pub(crate) struct DefaultMessageMapper;

impl MessageMapper for DefaultMessageMapper {
    fn create_mqtt_properties_from_frame_metadata(
        &self,
        header: &UFrameMetadata,
    ) -> Result<paho_mqtt::Properties, UStatus> {
        validate_header(header)?;

        let attributes = header.attributes();
        let mut properties = paho_mqtt::Properties::new();

        add_user_property(
            &mut properties,
            KEY_UPROTOCOL_VERSION,
            CURRENT_UPROTOCOL_MAJOR_VERSION.to_string().as_str(),
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
                        UCode::INTERNAL,
                        format!("Failed to create Message Expiry Interval property: {e:?}"),
                    )
                })?;

            if ttl % 1000 > 0 {
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
            "Failed to add message ID to MQTT User Properties",
        )?;
        add_user_property(
            &mut properties,
            KEY_TYPE,
            message_type_to_string(attributes.message_type()),
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
        add_user_property(
            &mut properties,
            KEY_PRIORITY,
            priority_to_string(attributes.priority()),
            "Failed to add message priority to MQTT User Properties",
        )?;

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
                &comm_status.as_u8().to_string(),
                "Failed to add message comm status to MQTT User Properties",
            )?;
        }
        if let Some(request_id) = attributes.request_id() {
            properties
                .push_binary::<Vec<u8>>(paho_mqtt::PropertyCode::CorrelationData, request_id.into())
                .map_err(|e| {
                    UStatus::fail_with_code(
                        UCode::INTERNAL,
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

        properties
            .push_string(
                paho_mqtt::PropertyCode::ContentType,
                header.encoding().content_type(),
            )
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Failed to create Content Type property: {e:?}"),
                )
            })?;
        add_user_property(
            &mut properties,
            KEY_ENCODING_FORMAT_ID,
            header.encoding().format_id(),
            "Failed to add payload format ID to MQTT User Properties",
        )?;
        if let Some(schema_ref) = header.encoding().schema_ref() {
            add_user_property(
                &mut properties,
                KEY_ENCODING_SCHEMA_REF,
                schema_ref,
                "Failed to add payload schema reference to MQTT User Properties",
            )?;
        }

        Ok(properties)
    }

    fn create_frame_metadata_from_mqtt_properties(
        &self,
        props: &paho_mqtt::Properties,
    ) -> Result<UFrameMetadata, UStatus> {
        let uprotocol_major_version = required_user_property(props, KEY_UPROTOCOL_VERSION)?
            .parse::<u8>()
            .map_err(|err| {
                UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    format!(
                        "Failed to map UserProperty {KEY_UPROTOCOL_VERSION} to uProtocol major version: {err}"
                    ),
                )
            })?;
        if uprotocol_major_version != CURRENT_UPROTOCOL_MAJOR_VERSION {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!("MQTT message contains unsupported uProtocol major version [expected: {CURRENT_UPROTOCOL_MAJOR_VERSION}, found: {uprotocol_major_version}]"),
            ));
        }

        let id = UUID::from_str(&required_user_property(props, KEY_MESSAGE_ID)?).map_err(|e| {
            UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!("Failed to map UserProperty {KEY_MESSAGE_ID} to Message ID: {e}"),
            )
        })?;
        let message_type = string_to_message_type(&required_user_property(props, KEY_TYPE)?)?;
        let source =
            UUri::from_str(&required_user_property(props, KEY_SOURCE)?).map_err(|err| {
                UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    format!("Failed to map UserProperty {KEY_SOURCE} to Message Source: {err}"),
                )
            })?;
        let sink = props
            .find_user_property(KEY_SINK)
            .map(|sink| {
                UUri::from_str(&sink).map_err(|err| {
                    UStatus::fail_with_code(
                        UCode::INVALID_ARGUMENT,
                        format!("Failed to map UserProperty {KEY_SINK} to Message Sink: {err}"),
                    )
                })
            })
            .transpose()?;

        let priority = props
            .find_user_property(KEY_PRIORITY)
            .map(|priority| string_to_priority(&priority))
            .transpose()?
            .unwrap_or_default();

        let mut attributes =
            UAttributes::new(id, source, sink, message_type).with_priority(priority);

        if let Some(ttl_string) = props.find_user_property(KEY_TTL) {
            attributes = attributes.with_ttl(ttl_string.parse::<u32>().map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    format!("Failed to map UserProperty {KEY_TTL} to Message TTL: {e}"),
                )
            })?);
        } else if let Some(message_expiry_interval) = props
            .get(paho_mqtt::PropertyCode::MessageExpiryInterval)
            .and_then(|prop| prop.get_u32())
        {
            attributes = attributes.with_ttl(message_expiry_interval.saturating_mul(1000));
        }

        if let Some(permission_string) = props.find_user_property(KEY_PERMISSION_LEVEL) {
            attributes = attributes.with_permission_level(permission_string.parse().map_err(|err| {
                UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    format!(
                        "Failed to map UserProperty {KEY_PERMISSION_LEVEL} to Permission Level: {err}"
                    ),
                )
            })?);
        }
        if let Some(comm_status_string) = props.find_user_property(KEY_COMMSTATUS) {
            let value = comm_status_string.parse::<u8>().map_err(|err| {
                UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    format!("Failed to map UserProperty {KEY_COMMSTATUS} to CommStatus: {err}"),
                )
            })?;
            attributes = attributes.with_commstatus(UCode::from_u8(value).ok_or_else(|| {
                UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    format!("Failed to map UserProperty {KEY_COMMSTATUS} to CommStatus: not a valid UCode [{value}]"),
                )
            })?);
        }
        if let Some(request_id) = props.get_binary(paho_mqtt::PropertyCode::CorrelationData) {
            attributes = attributes
                .with_request_id(UUID::try_from(request_id).map_err(|e| {
                    UStatus::fail_with_code(UCode::INVALID_ARGUMENT, e.to_string())
                })?);
        }
        if let Some(token) = props.find_user_property(KEY_TOKEN) {
            attributes = attributes.with_token(token);
        }
        if let Some(traceparent) = props.find_user_property(KEY_TRACEPARENT) {
            attributes = attributes.with_traceparent(traceparent);
        }
        if attributes.is_expired() {
            return Err(UStatus::fail_with_code(
                UCode::DEADLINE_EXCEEDED,
                "message has expired",
            ));
        }

        let content_type = props
            .get_string(paho_mqtt::PropertyCode::ContentType)
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let format_id = props
            .find_user_property(KEY_ENCODING_FORMAT_ID)
            .unwrap_or_else(|| content_type.clone());
        let schema_ref = props.find_user_property(KEY_ENCODING_SCHEMA_REF);

        let header = UFrameMetadata::new(
            attributes,
            UEncoding::new(format_id, content_type, schema_ref),
        );
        validate_header(&header)?;
        Ok(header)
    }
}

fn required_user_property(props: &paho_mqtt::Properties, key: &str) -> Result<String, UStatus> {
    props.find_user_property(key).ok_or_else(|| {
        UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("MQTT message does not contain required UserProperty {key}"),
        )
    })
}

fn validate_header(header: &UFrameMetadata) -> Result<(), UStatus> {
    UUri::check_validity(header.attributes().source()).map_err(|err| {
        UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("invalid message source URI: {err}"),
        )
    })?;
    if let Some(sink) = header.attributes().sink() {
        UUri::check_validity(sink).map_err(|err| {
            UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!("invalid message sink URI: {err}"),
            )
        })?;
    }
    if header.attributes().is_expired() {
        return Err(UStatus::fail_with_code(
            UCode::DEADLINE_EXCEEDED,
            "message has expired",
        ));
    }
    Ok(())
}

fn message_type_to_string(message_type: UMessageType) -> &'static str {
    match message_type {
        UMessageType::Publish => "publish",
        UMessageType::Notification => "notification",
        UMessageType::Request => "request",
        UMessageType::Response => "response",
    }
}

fn string_to_message_type(value: &str) -> Result<UMessageType, UStatus> {
    match value {
        "publish" => Ok(UMessageType::Publish),
        "notification" => Ok(UMessageType::Notification),
        "request" => Ok(UMessageType::Request),
        "response" => Ok(UMessageType::Response),
        _ => Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("invalid message type: {value}"),
        )),
    }
}

fn priority_to_string(priority: UPriority) -> &'static str {
    match priority {
        UPriority::CS0 => "CS0",
        UPriority::CS1 => "CS1",
        UPriority::CS2 => "CS2",
        UPriority::CS3 => "CS3",
        UPriority::CS4 => "CS4",
        UPriority::CS5 => "CS5",
        UPriority::CS6 => "CS6",
    }
}

fn string_to_priority(value: &str) -> Result<UPriority, UStatus> {
    match value {
        "CS0" => Ok(UPriority::CS0),
        "CS1" => Ok(UPriority::CS1),
        "CS2" => Ok(UPriority::CS2),
        "CS3" => Ok(UPriority::CS3),
        "CS4" => Ok(UPriority::CS4),
        "CS5" => Ok(UPriority::CS5),
        "CS6" => Ok(UPriority::CS6),
        _ => Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("invalid priority: {value}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_native_header_to_mqtt_properties_and_back() {
        let source = UUri::from_str("//vin.vehicles/A8000/2/8A50").unwrap();
        let sink = UUri::from_str("//backend/A8001/1/0").unwrap();
        let request_id = UUID::build();
        let attributes = UAttributes::new(UUID::build(), source, Some(sink), UMessageType::Request)
            .with_priority(UPriority::CS4)
            .with_ttl(3601)
            .with_request_id(request_id)
            .with_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
            .with_token("token")
            .with_permission_level(7)
            .with_commstatus(UCode::UNAVAILABLE);
        let header = UFrameMetadata::new(
            attributes,
            UEncoding::new(
                "custom-json",
                "application/custom+json",
                Some("schema://example/type"),
            ),
        );

        let mapper = DefaultMessageMapper;
        let properties = mapper
            .create_mqtt_properties_from_frame_metadata(&header)
            .unwrap();
        let mapped = mapper
            .create_frame_metadata_from_mqtt_properties(&properties)
            .unwrap();

        assert_eq!(&mapped, &header);
    }

    #[test]
    fn rejects_missing_native_metadata() {
        let mapper = DefaultMessageMapper;
        let properties = paho_mqtt::Properties::new();

        let error = mapper
            .create_frame_metadata_from_mqtt_properties(&properties)
            .unwrap_err();

        assert_eq!(error.get_code(), UCode::INVALID_ARGUMENT);
    }
}
