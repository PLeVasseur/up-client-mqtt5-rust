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

use std::{
    fs::OpenOptions,
    future::Future,
    io::Write,
    net::{Ipv4Addr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tempfile::{TempDir, TempPath};
use testcontainers::{
    core::{Mount, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use up_rust::UStatus;
use up_transport_mqtt5::{Mqtt5Transport, Mqtt5TransportOptions, MqttClientOptions, TransportMode};

const MOSQUITTO_CONTAINER_PORT: u16 = 1883;

/// Gets a future that completes once the given transport's connection to the MQTT broker has been (re-)established.
///
/// This function is useful for tests that need to wait for the MQTT connection to be ready before proceeding.
/// However, the future being returned is constantly polling the connection status,
/// which may not be the most efficient way to wait for the connection.
pub fn connection_established(transport: Arc<Mqtt5Transport>) -> impl Future<Output = ()> {
    std::future::poll_fn(move |cx| {
        if transport.is_connected() {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
}

pub async fn create_up_transport_mqtt<S: Into<String>>(
    authority_name: S,
    host_broker_port: u16,
    mqtt_client_id: Option<S>,
    username: Option<S>,
    password: Option<S>,
) -> Result<Mqtt5Transport, UStatus> {
    let mqtt_client_options = MqttClientOptions {
        // tcp or ssl
        // https://docs.rs/paho-mqtt/latest/paho_mqtt/create_options/struct.CreateOptionsBuilder.html#method.server_uri
        broker_uri: format!("tcp://localhost:{host_broker_port}"),
        clean_start: false,
        client_id: mqtt_client_id.map(|id| id.into()),
        max_buffered_messages: 100,
        session_expiry_interval: 3600,
        ssl_options: None,
        username: username.map(|v| v.into()),
        password: password.map(|v| v.into()),
    };
    let options = Mqtt5TransportOptions {
        mode: TransportMode::InVehicle,
        max_filters: 10,
        max_listeners_per_filter: 5,
        mqtt_client_options,
    };

    Mqtt5Transport::new(options, authority_name).await
}

pub(crate) struct MosquittoBroker {
    backend: BrokerBackend,
    port: u16,
    _config_path: TempPath,
    _passwords_path: TempPath,
    _acl_path: TempPath,
    _directory: TempDir,
}

enum BrokerBackend {
    Docker(ContainerAsync<GenericImage>),
    Native(Mutex<NativeMosquitto>),
}

struct NativeMosquitto {
    binary: PathBuf,
    config: PathBuf,
    log: PathBuf,
    port: u16,
    child: Option<Child>,
}

impl NativeMosquitto {
    fn start(&mut self) -> Result<(), String> {
        if self.child.is_some() {
            return Err("Mosquitto child has not been stopped".into());
        }
        let log = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.log)
            .map_err(|error| format!("failed to open Mosquitto log: {error}"))?;
        self.child = Some(
            Command::new(&self.binary)
                .arg("-c")
                .arg(&self.config)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::from(log))
                .spawn()
                .map_err(|error| format!("failed to spawn Mosquitto: {error}"))?,
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let child = self.child.as_mut().expect("owned Mosquitto child");
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("failed to inspect Mosquitto: {error}"))?
            {
                self.child = None;
                return Err(format!(
                    "Mosquitto exited with {status}: {}",
                    std::fs::read_to_string(&self.log).unwrap_or_default()
                ));
            }
            if std::fs::read_to_string(&self.log).is_ok_and(|log| log.contains(" running"))
                && TcpStream::connect_timeout(
                    &(Ipv4Addr::LOCALHOST, self.port).into(),
                    Duration::from_millis(100),
                )
                .is_ok()
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.stop();
        Err(format!(
            "Mosquitto readiness timed out: {}",
            std::fs::read_to_string(&self.log).unwrap_or_default()
        ))
    }

    fn stop(&mut self) -> Result<(), String> {
        let child = self.child.as_mut().ok_or("Mosquitto is not running")?;
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("failed to inspect Mosquitto: {error}"))?
        {
            self.child = None;
            return Err(format!("Mosquitto exited unexpectedly: {status}"));
        }
        let status = Command::new("/bin/kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .map_err(|error| format!("failed to signal owned Mosquitto child: {error}"))?;
        if !status.success() {
            return Err(format!("failed to stop owned Mosquitto child: {status}"));
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("failed to reap Mosquitto: {error}"))?
            {
                self.child = None;
                return if status.success() {
                    Ok(())
                } else {
                    Err(format!("Mosquitto failed during shutdown: {status}"))
                };
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err("owned Mosquitto child did not stop in time".into())
    }
}

impl Drop for NativeMosquitto {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl MosquittoBroker {
    pub(crate) fn port(&self) -> u16 {
        self.port
    }
    pub(crate) async fn start(&self) -> Result<(), String> {
        match &self.backend {
            BrokerBackend::Docker(broker) => broker
                .start()
                .await
                .map_err(|error| format!("Failed to start Mosquitto: {error}")),
            BrokerBackend::Native(broker) => {
                broker.lock().map_err(|error| error.to_string())?.start()
            }
        }
    }
    pub(crate) async fn stop(&self) -> Result<(), String> {
        match &self.backend {
            BrokerBackend::Docker(broker) => broker
                .stop()
                .await
                .map_err(|error| format!("Failed to stop Mosquitto: {error}")),
            BrokerBackend::Native(broker) => {
                broker.lock().map_err(|error| error.to_string())?.stop()
            }
        }
    }
}

/// Starts a private native Mosquitto child or an Eclipse Mosquitto Docker container.
///
/// # Arguments
/// * `config` - Additional Mosquitto configuration options.
/// * `passwords` - Credentials to include in the Mosquitto configuration.
///   If `None`, the broker will allow anonymous access.
/// * `acl_entries` - Access Control List definitions to include in the Mosquitto configuration.
/// * `mapped_port` - The host port to map the Mosquitto broker's container port to.
///   If `None`, the broker will be mapped to an ephemeral port on the host.
///
/// # Returns
/// A [MosquittoBroker] instance that owns the broker and its temporary files.
/// The broker is stopped when the returned instance is dropped.
///
/// The given configuration will be appended to the default Mosquitto configuration which is:
/// ```text
/// listener 1883
/// ```
///
/// `UP_MQTT5_MOSQUITTO_BIN` selects a native executable instead of Docker.
/// Native listeners bind only to loopback and use private persistence and credential files.
pub(crate) async fn start_mosquitto(
    config: Option<&str>,
    passwords: Option<&str>,
    acl_entries: Option<&str>,
    mapped_port: Option<u16>,
) -> MosquittoBroker {
    let native_binary = std::env::var_os("UP_MQTT5_MOSQUITTO_BIN").map(PathBuf::from);
    let directory = tempfile::tempdir().expect("failed to create private Mosquitto directory");
    let reservation = native_binary.as_ref().map(|_| {
        TcpListener::bind((Ipv4Addr::LOCALHOST, mapped_port.unwrap_or_default()))
            .expect("native Mosquitto port is not exclusively available")
    });
    let native_port = reservation.as_ref().map(|listener| {
        listener
            .local_addr()
            .expect("reserved broker address")
            .port()
    });
    let mut mosquitto_config = tempfile::NamedTempFile::new_in(directory.path())
        .expect("failed to create Mosquitto config file");
    if let Some(port) = native_port {
        writeln!(mosquitto_config, "listener {port} 127.0.0.1")
            .expect("failed to write to Mosquitto config file");
    } else {
        writeln!(mosquitto_config, "listener 1883")
            .expect("failed to write to Mosquitto config file");
    }

    if let Some(configuration) = config {
        for line in configuration.lines() {
            if native_binary.is_some() && line.trim_start().starts_with("persistence_location ") {
                writeln!(
                    mosquitto_config,
                    "persistence_location {}/",
                    directory.path().display()
                )
                .expect("failed to write native Mosquitto persistence location");
            } else {
                writeln!(mosquitto_config, "{line}")
                    .expect("failed to write to Mosquitto config file");
            }
        }
    }

    let mut mosquitto_passwords = tempfile::NamedTempFile::new_in(directory.path())
        .expect("failed to create Mosquitto password file");
    if let Some(pwd) = passwords {
        writeln!(mosquitto_passwords, "{pwd}").expect("failed to write to Mosquitto password file");
        writeln!(mosquitto_config, "allow_anonymous false")
            .expect("failed to write to Mosquitto config file");
        let password_file = if native_binary.is_some() {
            mosquitto_passwords.path().display().to_string()
        } else {
            "/mosquitto/config/passwords".into()
        };
        writeln!(mosquitto_config, "password_file {password_file}")
            .expect("failed to write to Mosquitto config file");
    } else {
        writeln!(mosquitto_config, "allow_anonymous true")
            .expect("failed to write to Mosquitto config file");
    }

    let mut mosquitto_acls = tempfile::NamedTempFile::new_in(directory.path())
        .expect("failed to create Mosquitto ACL file");
    if let Some(acls) = acl_entries {
        writeln!(mosquitto_acls, "{acls}").expect("failed to write to Mosquitto ACL file");
        let acl_file = if native_binary.is_some() {
            mosquitto_acls.path().display().to_string()
        } else {
            "/mosquitto/config/acls".into()
        };
        writeln!(mosquitto_config, "acl_file {acl_file}")
            .expect("failed to write to Mosquitto config file");
    }

    let config_path = mosquitto_config.into_temp_path();
    let password_path = mosquitto_passwords.into_temp_path();
    let acl_path = mosquitto_acls.into_temp_path();
    if let Some(binary) = native_binary {
        let port = native_port.expect("reserved native broker port");
        let mut broker = NativeMosquitto {
            binary,
            config: config_path.to_path_buf(),
            log: directory.path().join("mosquitto.log"),
            port,
            child: None,
        };
        drop(reservation);
        broker.start().expect("failed to start native Mosquitto");
        return MosquittoBroker {
            backend: BrokerBackend::Native(Mutex::new(broker)),
            port,
            _config_path: config_path,
            _passwords_path: password_path,
            _acl_path: acl_path,
            _directory: directory,
        };
    }
    let container_port = MOSQUITTO_CONTAINER_PORT.into();
    let container = GenericImage::new("eclipse-mosquitto", "2.0")
        .with_exposed_port(container_port)
        // mosquitto seems to write to stderr
        .with_wait_for(WaitFor::message_on_stderr(" running"))
        // uncomment next line in order capture Mosquitto log messages
        .with_log_consumer(
            testcontainers::core::logs::consumer::logging_consumer::LoggingConsumer::new(),
        )
        // use given configuration
        .with_mount(Mount::bind_mount(
            config_path.display().to_string(),
            "/mosquitto/config/mosquitto.conf",
        ))
        .with_mount(Mount::bind_mount(
            password_path.display().to_string(),
            "/mosquitto/config/passwords",
        ))
        .with_mount(Mount::bind_mount(
            acl_path.display().to_string(),
            "/mosquitto/config/acls",
        ))
        .with_mapped_port(mapped_port.unwrap_or_default(), container_port)
        .start()
        .await
        .expect("Failed to start Mosquitto");

    let host_port = container
        .get_host_port_ipv4(MOSQUITTO_CONTAINER_PORT)
        .await
        .expect("MQTT port {MOSQUITTO_CONTAINER_PORT} not exposed");

    MosquittoBroker {
        backend: BrokerBackend::Docker(container),
        port: host_port,
        _config_path: config_path,
        _passwords_path: password_path,
        _acl_path: acl_path,
        _directory: directory,
    }
}
