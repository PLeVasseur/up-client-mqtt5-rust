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
    env,
    future::Future,
    io::Write,
    net::{TcpListener, TcpStream},
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
const BROKER_READY_TIMEOUT: Duration = Duration::from_secs(10);

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
    backend: MosquittoBrokerBackend,
    port: u16,
}

enum MosquittoBrokerBackend {
    Docker {
        broker: ContainerAsync<GenericImage>,
        _config_path: TempPath,
        _passwords_path: TempPath,
        _acl_path: TempPath,
    },
    Native {
        command: PathBuf,
        config_path: PathBuf,
        child: Mutex<Option<Child>>,
        _temp_dir: TempDir,
    },
}

impl MosquittoBroker {
    pub(crate) fn port(&self) -> u16 {
        self.port
    }
    pub(crate) async fn start(&self) -> Result<(), String> {
        match &self.backend {
            MosquittoBrokerBackend::Docker { broker, .. } => broker
                .start()
                .await
                .map_err(|e| format!("Failed to start Mosquitto: {e}")),
            MosquittoBrokerBackend::Native { .. } => self.start_native(),
        }
    }
    pub(crate) async fn stop(&self) -> Result<(), String> {
        match &self.backend {
            MosquittoBrokerBackend::Docker { broker, .. } => broker
                .stop()
                .await
                .map_err(|e| format!("Failed to stop Mosquitto: {e}")),
            MosquittoBrokerBackend::Native { child, .. } => stop_native_child(child),
        }
    }

    fn start_native(&self) -> Result<(), String> {
        let MosquittoBrokerBackend::Native {
            command,
            config_path,
            child,
            ..
        } = &self.backend
        else {
            return Ok(());
        };

        {
            let guard = child
                .lock()
                .map_err(|_| "Mosquitto child process lock poisoned".to_string())?;
            if guard.is_some() {
                return Ok(());
            }
        }

        let spawned = Command::new(command)
            .arg("-c")
            .arg(config_path)
            .arg("-v")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("Failed to start native Mosquitto: {e}"))?;

        {
            let mut guard = child
                .lock()
                .map_err(|_| "Mosquitto child process lock poisoned".to_string())?;
            *guard = Some(spawned);
        }

        if let Err(err) = wait_for_broker(self.port) {
            let _ = stop_native_child(child);
            return Err(err);
        }
        Ok(())
    }
}

impl Drop for MosquittoBroker {
    fn drop(&mut self) {
        if let MosquittoBrokerBackend::Native { child, .. } = &self.backend {
            let _ = stop_native_child(child);
        }
    }
}

/// Starts an Eclipse Mosquitto Docker container based on given configuration options.
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
/// A [MosquittoBroker] instance that represents the running Mosquitto container.
/// The container will be stopped and removed when the returned instance is dropped.
///
/// The given configuration will be appended to the default Mosquitto configuration which is:
/// ```text
/// listener 1883
/// ```
///
/// The configuration is written to a temporary file which is bind-mounted into the container.
pub(crate) async fn start_mosquitto(
    config: Option<&str>,
    passwords: Option<&str>,
    acl_entries: Option<&str>,
    mapped_port: Option<u16>,
) -> MosquittoBroker {
    if use_native_mosquitto() {
        return start_native_mosquitto(config, passwords, acl_entries, mapped_port);
    }

    let mut mosquitto_config =
        tempfile::NamedTempFile::new().expect("failed to create Mosquitto config file");
    writeln!(mosquitto_config, "listener 1883").expect("failed to write to Mosquitto config file");

    if let Some(configuration) = config {
        writeln!(mosquitto_config, "{configuration}")
            .expect("failed to write to Mosquitto config file");
    }

    let mut mosquitto_passwords =
        tempfile::NamedTempFile::new().expect("failed to create Mosquitto password file");
    if let Some(pwd) = passwords {
        writeln!(mosquitto_passwords, "{pwd}").expect("failed to write to Mosquitto password file");
        writeln!(mosquitto_config, "allow_anonymous false")
            .expect("failed to write to Mosquitto config file");
        writeln!(
            mosquitto_config,
            "password_file /mosquitto/config/passwords"
        )
        .expect("failed to write to Mosquitto config file");
    } else {
        writeln!(mosquitto_config, "allow_anonymous true")
            .expect("failed to write to Mosquitto config file");
    }

    let mut mosquitto_acls =
        tempfile::NamedTempFile::new().expect("failed to create Mosquitto ACL file");
    if let Some(acls) = acl_entries {
        writeln!(mosquitto_acls, "{acls}").expect("failed to write to Mosquitto ACL file");
        writeln!(mosquitto_config, "acl_file /mosquitto/config/acls")
            .expect("failed to write to Mosquitto config file");
    }

    let config_path = mosquitto_config.into_temp_path();
    let password_path = mosquitto_passwords.into_temp_path();
    let acl_path = mosquitto_acls.into_temp_path();
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
        backend: MosquittoBrokerBackend::Docker {
            broker: container,
            _config_path: config_path,
            _passwords_path: password_path,
            _acl_path: acl_path,
        },
        port: host_port,
    }
}

fn use_native_mosquitto() -> bool {
    matches!(
        env::var("UP_MQTT5_TEST_BROKER_MODE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "native"
    )
}

fn start_native_mosquitto(
    config: Option<&str>,
    passwords: Option<&str>,
    acl_entries: Option<&str>,
    mapped_port: Option<u16>,
) -> MosquittoBroker {
    let temp_dir = tempfile::tempdir().expect("failed to create Mosquitto temp directory");
    let port = mapped_port.unwrap_or_else(find_available_port);
    let config_path = temp_dir.path().join("mosquitto.conf");
    let password_path = temp_dir.path().join("passwords");
    let acl_path = temp_dir.path().join("acls");
    let persistence_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&persistence_path).expect("failed to create Mosquitto data directory");

    let mut mosquitto_config =
        std::fs::File::create(&config_path).expect("failed to create native Mosquitto config file");
    writeln!(mosquitto_config, "listener {port}")
        .expect("failed to write native Mosquitto config file");

    if let Some(configuration) = config {
        let native_config = configuration.replace(
            "/mosquitto/data/",
            &format!("{}/", persistence_path.display()),
        );
        writeln!(mosquitto_config, "{native_config}")
            .expect("failed to write native Mosquitto config file");
    }

    if let Some(pwd) = passwords {
        std::fs::write(&password_path, format!("{pwd}\n"))
            .expect("failed to write native Mosquitto password file");
        writeln!(mosquitto_config, "allow_anonymous false")
            .expect("failed to write native Mosquitto config file");
        writeln!(
            mosquitto_config,
            "password_file {}",
            password_path.display()
        )
        .expect("failed to write native Mosquitto config file");
    } else {
        writeln!(mosquitto_config, "allow_anonymous true")
            .expect("failed to write native Mosquitto config file");
    }

    if let Some(acls) = acl_entries {
        std::fs::write(&acl_path, format!("{acls}\n"))
            .expect("failed to write native Mosquitto ACL file");
        writeln!(mosquitto_config, "acl_file {}", acl_path.display())
            .expect("failed to write native Mosquitto config file");
    }

    drop(mosquitto_config);

    let command = resolve_mosquitto_command();
    let broker = MosquittoBroker {
        backend: MosquittoBrokerBackend::Native {
            command,
            config_path,
            child: Mutex::new(None),
            _temp_dir: temp_dir,
        },
        port,
    };
    broker.start_native().expect("Failed to start Mosquitto");
    broker
}

fn resolve_mosquitto_command() -> PathBuf {
    env::var_os("UP_MQTT5_TEST_MOSQUITTO")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("mosquitto"))
}

fn find_available_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("failed to bind ephemeral port")
        .local_addr()
        .expect("failed to read ephemeral port")
        .port()
}

fn wait_for_broker(port: u16) -> Result<(), String> {
    let deadline = Instant::now() + BROKER_READY_TIMEOUT;
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for native Mosquitto on localhost:{port}"
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn stop_native_child(child: &Mutex<Option<Child>>) -> Result<(), String> {
    let mut child = child
        .lock()
        .map_err(|_| "Mosquitto child process lock poisoned".to_string())?;
    if let Some(mut process) = child.take() {
        process
            .kill()
            .map_err(|e| format!("Failed to stop native Mosquitto: {e}"))?;
        process
            .wait()
            .map_err(|e| format!("Failed to wait for native Mosquitto: {e}"))?;
    }
    Ok(())
}
