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
//! Detect broker availability for the transport's integration tests.
use std::{path::PathBuf, process::Command};

/// Enables broker tests for an explicitly selected native broker or a prepared Docker image.
///
/// This check never pulls an image. Set `UP_MQTT5_REQUIRE_BROKER=1` in acceptance
/// runs to fail the build if the daemon/image is unavailable rather than omit tests.
pub fn main() {
    link_external_paho_ssl();
    let required = match std::env::var("UP_MQTT5_REQUIRE_BROKER") {
        Ok(value) if value == "1" => true,
        Ok(value) if value == "0" => false,
        Err(std::env::VarError::NotPresent) => false,
        _ => panic!("UP_MQTT5_REQUIRE_BROKER must be 0 or 1"),
    };
    let available = match std::env::var_os("UP_MQTT5_MOSQUITTO_BIN") {
        Some(binary) => {
            assert!(
                PathBuf::from(&binary).is_absolute(),
                "UP_MQTT5_MOSQUITTO_BIN must be an absolute executable path"
            );
            let output = Command::new(binary)
                .arg("-h")
                .output()
                .expect("failed to execute UP_MQTT5_MOSQUITTO_BIN");
            assert!(
                (output.status.success() || output.status.code() == Some(3))
                    && String::from_utf8_lossy(&output.stdout).contains("mosquitto version"),
                "UP_MQTT5_MOSQUITTO_BIN must select a working Mosquitto executable"
            );
            true
        }
        None => Command::new("docker")
            .args(["image", "inspect", "eclipse-mosquitto:2.0"])
            .output()
            .is_ok_and(|output| output.status.success()),
    };
    if available {
        println!("cargo::rustc-cfg=docker_available");
    } else if required {
        panic!("broker acceptance requires UP_MQTT5_MOSQUITTO_BIN or Docker with the prepared eclipse-mosquitto:2.0 image");
    } else {
        println!(
            "cargo::warning=broker tests require UP_MQTT5_MOSQUITTO_BIN or Docker with a prepared eclipse-mosquitto:2.0 image"
        );
    }
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed=DOCKER_HOST");
    println!("cargo::rerun-if-env-changed=UP_MQTT5_REQUIRE_BROKER");
    println!("cargo::rerun-if-env-changed=UP_MQTT5_MOSQUITTO_BIN");
}

fn link_external_paho_ssl() {
    for variable in [
        "PAHO_MQTT_C_LIB_DIR",
        "OPENSSL_DIR",
        "OPENSSL_LIB_DIR",
        "OPENSSL_STATIC",
    ] {
        println!("cargo::rerun-if-env-changed={variable}");
    }
    if std::env::var_os("PAHO_MQTT_C_LIB_DIR").is_none() {
        return;
    }
    let Some(lib_dir) = std::env::var_os("OPENSSL_LIB_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("OPENSSL_DIR").map(|root| PathBuf::from(root).join("lib")))
    else {
        return;
    };
    println!("cargo::rustc-link-search=native={}", lib_dir.display());
    let linkage = match std::env::var("OPENSSL_STATIC").as_deref() {
        Ok("0") | Err(_) => "",
        Ok(_) => "static=",
    };
    println!("cargo::rustc-link-lib={linkage}ssl");
    println!("cargo::rustc-link-lib={linkage}crypto");
}
