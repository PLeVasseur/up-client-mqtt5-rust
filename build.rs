/********************************************************************************
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
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
//! Detect Docker-backed broker availability for the transport's integration tests.
use std::process::Command;

/// Enables broker tests when the selected Docker daemon has the prepared image.
///
/// This check never pulls an image. Set `UP_MQTT5_REQUIRE_BROKER=1` in acceptance
/// runs to fail the build if the daemon/image is unavailable rather than omit tests.
pub fn main() {
    let required = match std::env::var("UP_MQTT5_REQUIRE_BROKER") {
        Ok(value) if value == "1" => true,
        Ok(value) if value == "0" => false,
        Err(std::env::VarError::NotPresent) => false,
        _ => panic!("UP_MQTT5_REQUIRE_BROKER must be 0 or 1"),
    };
    let available = Command::new("docker")
        .args(["image", "inspect", "eclipse-mosquitto:2.0"])
        .output()
        .is_ok_and(|output| output.status.success());
    if available {
        println!("cargo::rustc-cfg=docker_available");
    } else if required {
        panic!("broker acceptance requires Docker and the prepared eclipse-mosquitto:2.0 image");
    } else {
        println!(
            "cargo::warning=broker tests require Docker and a prepared eclipse-mosquitto:2.0 image"
        );
    }
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed=DOCKER_HOST");
    println!("cargo::rerun-if-env-changed=UP_MQTT5_REQUIRE_BROKER");
}
