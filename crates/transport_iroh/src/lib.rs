#![deny(missing_docs)]
//! kitsune2 iroh transport module.

use base64::Engine;
use iroh::{
    endpoint::{Connection, SendStream, StoppedError, VarInt},
    net_report::Report,
    Endpoint, NodeAddr, NodeId, RelayMap, RelayMode, RelayUrl, Watcher,
};
use kitsune2_api::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::{sync::Mutex, task::AbortHandle};

#[allow(missing_docs)]
pub mod config {
    /// Configuration parameters for [IrohTransportFactory](super::IrohTransportFactory).
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct IrohTransportConfig {
        pub custom_relay_url: Option<String>,
    }

    impl Default for IrohTransportConfig {
        fn default() -> Self {
            Self {
                custom_relay_url: None,
            }
        }
    }

    /// Module-level configuration for IrohTransport.
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct IrohTransportModConfig {
        /// IrohTransport configuration.
        pub iroh_transport: IrohTransportConfig,
    }
}

pub use config::*;
/// Provides a Kitsune2 transport module based on the iroh crate.
#[derive(Debug)]
pub struct IrohTransportFactory {}

impl IrohTransportFactory {
    /// Construct a new IrohTransportFactory.
    pub fn create() -> DynTransportFactory {
        let out: DynTransportFactory = Arc::new(IrohTransportFactory {});
        out
    }
}

impl TransportFactory for IrohTransportFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        config.set_module_config(&IrohTransportModConfig::default())
    }

    fn validate_config(&self, config: &Config) -> K2Result<()> {
        let config: IrohTransportModConfig = config.get_module_config()?;

        // make sure our relay server url is parse-able.i
        if let Some(relay_url) = config.iroh_transport.custom_relay_url {
            let _sig = url::Url::parse(relay_url.as_str()).map_err(|err| {
                K2Error::other_src("invalid iroh custom relay url", err)
            })?;
        }

        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        handler: DynTxHandler,
    ) -> BoxFut<'static, K2Result<DynTransport>> {
        Box::pin(async move {
            let config: IrohTransportModConfig =
                builder.config.get_module_config()?;

            let handler = TxImpHnd::new(handler);
            let imp =
                IrohTransport::create(config.iroh_transport, handler.clone())
                    .await?;
            Ok(DefaultTransport::create(&handler, imp))
        })
    }
}

const ALPN: &[u8] = b"kitsune2";

#[derive(Debug, Clone)]
struct PeerConnection {
    connection: Connection,
    recv_abort_handle: AbortHandle,
    current_peer_url: Url,
}

struct IrohTransport {
    handler: Arc<TxImpHnd>,
    endpoint: Arc<Endpoint>,
    connections: Arc<Mutex<BTreeMap<NodeId, PeerConnection>>>,
    tasks: Vec<AbortHandle>,
}

impl IrohTransport {
    async fn open_send_stream(
        &self,
        peer_url: Url,
    ) -> Result<SendStream, K2Error> {
        let node_id = peer_url_to_node_id(peer_url.clone())
            .map_err(|err| K2Error::other_src("bad peer url", err))?;

        let mut connections = self.connections.lock().await;

        let connection = if let Some(connection) = connections.get_mut(&node_id)
        {
            connection.current_peer_url = peer_url.clone();
            connection.connection.clone()
        } else {
            drop(connections);
            let node_addr = peer_url_to_node_addr(peer_url.clone())
                .map_err(|err| K2Error::other_src("bad peer url", err))?;

            tracing::info!("Connecting to peer url {peer_url} through node address {node_addr:?}.");

            let connection = match self
                .endpoint
                .connect(node_addr.clone(), ALPN)
                .await
            {
                Ok(c) => c,
                Err(err) => {
                    tracing::warn!(
                        "connect() failed: marking {peer_url} as unresponsive"
                    );
                    self.handler
                        .set_unresponsive(peer_url.clone(), Timestamp::now())
                        .await?;
                    return Err(K2Error::other(format!(
                        "failed to connect: {err:?}"
                    )));
                }
            };

            tracing::debug!("Connect with {peer_url} successful.");

            let recv_abort_handle = setup_incoming_listener(
                self.endpoint.clone(),
                self.connections.clone(),
                &connection,
                self.handler.clone(),
            );
            let mut connections = self.connections.lock().await;
            if let Some(c) = connections.get(&node_id) {
                c.recv_abort_handle.abort();
                c.connection.close(VarInt::from_u32(0), b"disconnected");
            }
            connections.insert(
                node_id.clone(),
                PeerConnection {
                    connection: connection.clone(),
                    recv_abort_handle,
                    current_peer_url: peer_url.clone(),
                },
            );
            connection
        };

        match connection.open_uni().await {
            Ok(s) => {
                tracing::debug!("open_uni() to {peer_url} successful.");

                Ok(s)
            }
            Err(err) => {
                tracing::info!("open_uni() with {peer_url} failed: {err:?}. Marking {peer_url} as unresponsive.");
                self.handler
                    .set_unresponsive(peer_url.clone(), Timestamp::now())
                    .await?;
                return Err(K2Error::other(format!(
                    "failed to open_uni(): {err:?}"
                )));
            }
        }
    }
}

impl std::fmt::Debug for IrohTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IrohTransport {{
                endpoint: {:?},
                connections: {:?},
            }}",
            self.endpoint, self.connections
        )
    }
}

impl Drop for IrohTransport {
    fn drop(&mut self) {
        for task in &mut self.tasks {
            task.abort();
        }
        tokio::runtime::Handle::current().block_on(async move {
            self.endpoint.close().await;
        });
    }
}

impl IrohTransport {
    pub async fn create(
        config: IrohTransportConfig,
        handler: Arc<TxImpHnd>,
    ) -> K2Result<DynTxImp> {
        let relay_mode = match config.custom_relay_url {
            Some(relay_url_str) => {
                let relay_url = url::Url::parse(relay_url_str.as_str())
                    .map_err(|err| {
                        K2Error::other_src(
                            "Failed to parse custom relay url",
                            err,
                        )
                    })?;
                RelayMode::Custom(RelayMap::from(RelayUrl::from(relay_url)))
            }
            None => RelayMode::Default,
        };
        let endpoint = iroh::Endpoint::builder()
            .relay_mode(relay_mode)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .map_err(|err| {
                K2Error::other_src("failed to bind endpoint", err)
            })?;

        let endpoint = Arc::new(endpoint);

        let connections = Arc::new(Mutex::new(BTreeMap::new()));
        let h = handler.clone();
        let e = endpoint.clone();
        let watch_relay_task = tokio::spawn(async move {
            loop {
                match e.home_relay().updated().await {
                    Ok(_) => {
                        
                        let url = match get_endpoint_peer_url(e.clone()) {
                            Ok(u) => u,
                            Err(err) => {
                                tracing::error!(
                                    "Failed to get my endpoint peer url: {err}."
                                );
                                continue;
                            }
                        };
                        tracing::info!("New listening address: {}.", url);
                        h.new_listening_address(url).await;
                    }
                    Err(err) => {
                        tracing::error!(
                            "Failed to get new relay url: {err:?}."
                        );
                    }
                }
            }
        })
        .abort_handle();

        // If we change our offline/online status, create new agent info
        let h = handler.clone();
        let e = endpoint.clone();
        let net_report_task = tokio::spawn(async move {
            let mut maybe_last_report: Option<Report> = None;
            loop {
                match e.net_report().updated().await {
                    Ok(Some(report)) => {

                        tracing::info!("New network report: {report:?}.");

                        let lr = maybe_last_report.clone();
                        maybe_last_report = Some(report.clone());

                        match lr {
                            Some(last_report) => {
                                if last_report.udp_v4 && report.udp_v4 {
                                    continue;
                                }
                            }
                            None => {}
                        }

                        tracing::warn!(
                            "Network changed! Online status: {}.",
                            report.udp_v4, 
                        );

                        let url = match get_endpoint_peer_url(e.clone()) {
                            Ok(url) => url,
                            Err(err) => {
                                tracing::error!("Failed to get my endpoint peer url: {err:?}.");
                                continue;
                            }
                        };
                        tracing::info!("New listening address: {}.", url);
                        h.new_listening_address(url).await;
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::error!("Failed to get net report: {err:?}.");
                    }
                }
            }
        })
        .abort_handle();

        let evt_task = tokio::task::spawn(evt_task(
            connections.clone(),
            handler.clone(),
            endpoint.clone(),
        ))
        .abort_handle();

        let out: DynTxImp = Arc::new(Self {
            handler,
            endpoint,
            connections,
            tasks: vec![watch_relay_task, net_report_task, evt_task],
        });

        Ok(out)
    }
}

fn get_endpoint_peer_url(endpoint: Arc<Endpoint>) -> Result<Url, K2Error> {
    let report = endpoint.net_report().get();
    let relays = endpoint.home_relay().get().first().cloned();
    let url: url::Url = match (relays, report) {
        (Some(relay_url), None) => relay_url.clone().into(),
        (Some(relay_url), Some(report)) if report.udp_v4 => relay_url.clone().into(),
        _ => {
            let local_ip_addresses = endpoint.bound_sockets();

            let local_address = if let Ok(local_ip) = local_ip_address::local_ip() {
                tracing::warn!("Localip {local_ip}");
                let bound_ip_adress = local_ip_addresses
                    .iter().find(|addr| addr.to_string().contains(&local_ip.to_string()));
        
                if let Some(bound_local_ip) = bound_ip_adress {
                    Some(bound_local_ip)
                } else {
                    local_ip_addresses.first()
                }
            } else {
                    local_ip_addresses.first()
            };
            let Some(local_address ) = local_address else {
                return Err(K2Error::other("We don't have any local addresses bound nor home relay: we can't connect to any peers."));
            };
            let Ok(url) = url::Url::parse(format!("http://{}:{}", local_address.ip(), local_address.port()).as_str()) else {
                return Err(K2Error::other("Parse error for local address: {local_address}."));
            };
            url
        }
    };
    to_peer_url(url.clone().into(), endpoint.node_id())
}

fn peer_url_to_node_addr(peer_url: Url) -> Result<NodeAddr, K2Error> {
    let url = url::Url::parse(peer_url.as_str()).map_err(|err| {
        K2Error::other(format!("Failed to parse peer url: {err:?}"))
    })?;
    let node_id = peer_url_to_node_id(peer_url.clone())?;

    if url.domain().is_none() {
        let direct_address = url::Url::parse(
            format!("{}://{}", url.scheme(), peer_url.addr()).as_str(),
        )
        .map_err(|err| K2Error::other_src("Bad addr", err))?;
        let direct_addresses: BTreeSet<SocketAddr> = direct_address
            .socket_addrs(|| None)
            .map_err(|err| K2Error::other_src("Invalid socket addrs", err))?
            .into_iter()
            .collect();

        Ok(NodeAddr {
            node_id,
            relay_url: None,
            direct_addresses,
        })
    } else {
        let relay_url = url::Url::parse(
            format!("{}://{}", url.scheme(), peer_url.addr()).as_str(),
        )
        .map_err(|err| K2Error::other_src("Bad addr", err))?;

        Ok(NodeAddr {
            node_id,
            relay_url: Some(RelayUrl::from(relay_url)),
            direct_addresses: BTreeSet::new(),
        })
    }
}

fn peer_url_to_node_id(peer_url: Url) -> Result<NodeId, K2Error> {
    let Some(peer_id) = peer_url.peer_id() else {
        return Err(K2Error::other("empty peer url"));
    };
    let decoded_peer_id = base64::prelude::BASE64_URL_SAFE_NO_PAD
        .decode(peer_id)
        .map_err(|err| K2Error::other_src("failed to decode peer id", err))?;
    let node_id = NodeId::try_from(decoded_peer_id.as_slice())
        .map_err(|err| K2Error::other_src("bad peer id", err))?;

    Ok(node_id)
}

fn to_peer_url(url: url::Url, node_id: NodeId) -> Result<Url, K2Error> {
    let port = match url.port() {
        Some(_) => format!(""),
        None => format!(":443"),
    };

    let mut url_str = url.to_string();
    if let Some(s) = url_str.strip_suffix("./") {
        url_str = s.to_string();
    }
    if let Some(s) = url_str.strip_suffix("/") {
        url_str = s.to_string();
    }
    let u = format!(
        "{url_str}{port}/{}",
        base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(node_id)
    );
    Url::from_str(u.as_str())
}

fn node_addr_to_peer_url(node_addr: NodeAddr) -> Result<Url, K2Error> {
    match node_addr.relay_url {
        Some(relay_url) => to_peer_url(relay_url.into(), node_addr.node_id),
        None => {
            let direct_addresses = node_addr
                .direct_addresses
                .into_iter()
                .collect::<Vec<SocketAddr>>();
            let local_192_address = direct_addresses.iter()
                .find(|a| a.to_string().starts_with("192."));
            let local_172_address = direct_addresses.iter()
                .find(|a| a.to_string().starts_with("172."));
            let local_10_address = direct_addresses.iter()
                .find(|a| a.to_string().starts_with("10."));
            let Some(direct_address) = local_192_address.or(local_172_address)
                .or(local_10_address)
                .or(direct_addresses.first())
                .cloned() else {
                return Err(K2Error::other(
                    "node addr has no relay url and no direct addresses",
                ));
            };
            let url = url::Url::parse(
                format!(
                    "http://{}:{}",
                    direct_address.ip().to_string().replace("/", ""),
                    direct_address.port()
                )
                .as_str(),
            )
            .map_err(|err| {
                K2Error::other_src(
                    "Failed to parse direct address into URL.",
                    err,
                )
            })?;
            to_peer_url(url, node_addr.node_id)
        }
    }
}

impl TxImp for IrohTransport {
    fn url(&self) -> Option<Url> {
        let my_node_addr = self.endpoint.node_addr().get()?;
        let peer_url = match node_addr_to_peer_url(my_node_addr) {
            Ok(u) => u,
            Err(err) => {
                tracing::error!(
                    "Failed to convert node address to peer url: {err}."
                );
                return None;
            }
        };

        tracing::info!("My peer URL: {peer_url}.");

        Some(peer_url)
    }

    fn disconnect(
        &self,
        peer: Url,
        _payload: Option<(String, bytes::Bytes)>,
    ) -> BoxFut<'_, ()> {
        Box::pin(async move {
            tracing::debug!("Disconnecting from {peer}.");
            let Ok(node_id) = peer_url_to_node_id(peer) else {
                tracing::error!("Bad peer url to node addr");
                return;
            };
            let mut connections = self.connections.lock().await;
            if let Some(peer_connection) = connections.get(&node_id) {
                peer_connection
                    .connection
                    .close(VarInt::from_u32(0), b"disconnected");
                peer_connection.recv_abort_handle.abort();
                connections.remove(&node_id);
            }
            ()
        })
    }

    fn send(&self, peer: Url, data: bytes::Bytes) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            tracing::debug!("Attempting to send message to {peer}.");

            let mut send = self.open_send_stream(peer.clone()).await?;

            send.write_all(data.as_ref()).await.map_err(|err| {
                K2Error::other_src("Failed to write all", err)
            })?;
            send.finish().map_err(|err| {
                K2Error::other_src("Failed to close stream", err)
            })?;
            if let Err(err) = send.stopped().await {
                tracing::warn!(
                    "stopped() failed: marking {peer} as unresponsive"
                );
                self.handler
                    .set_unresponsive(peer.clone(), Timestamp::now())
                    .await?;
                if let StoppedError::ConnectionLost(_) = err {
                    let mut connections = self.connections.lock().await;
                    let node_id = peer_url_to_node_id(peer.clone())?;
                    if let Some(connection) = connections.get(&node_id) {
                        connection
                            .connection
                            .close(VarInt::from_u32(0), b"disconnected");
                        connections.remove(&node_id);
                    }
                }
                return Err(K2Error::other_src("error stopping", err));
            }

            tracing::debug!("Write all with {peer} successful.");
            Ok(())
        })
    }

    fn dump_network_stats(&self) -> BoxFut<'_, K2Result<TransportStats>> {
        Box::pin(async move {
            let connections = self.connections.lock().await;
            let peer_urls: BTreeSet<Url> = connections
                .iter()
                .filter_map(|(node_id, _)| {
                    let remote_info =
                        self.endpoint.remote_info(node_id.clone())?;

                    node_addr_to_peer_url(remote_info.into()).ok()
                })
                .collect();

            Ok(TransportStats {
                backend: format!("iroh"),
                peer_urls: peer_urls.into_iter().collect(),
                connections: connections
                    .iter()
                    .map(|(node_id, conn)| TransportConnectionStats {
                        pub_key: base64::prelude::BASE64_URL_SAFE_NO_PAD
                            .encode(node_id),
                        send_message_count: 0,
                        send_bytes: 0,
                        recv_message_count: 0,
                        recv_bytes: 0,
                        opened_at_s: 0,
                        is_webrtc: false,
                    })
                    .collect(),
            })
        })
    }

    fn get_connected_peers(&self) -> BoxFut<'_, K2Result<Vec<Url>>> {
        Box::pin(async move {
            let connections = self.connections.lock().await;
            Ok(connections
                .values()
                .map(|peer_connection| peer_connection.current_peer_url.clone())
                .collect())
        })
    }
}

async fn evt_task(
    connections: Arc<Mutex<BTreeMap<NodeId, PeerConnection>>>,
    handler: Arc<TxImpHnd>,
    endpoint: Arc<Endpoint>,
) {
    while let Some(incoming) = endpoint.accept().await {
        let endpoint = endpoint.clone();
        let handler = handler.clone();
        let connections = connections.clone();
        tokio::spawn(async move {
            let connection = match incoming.await {
                Ok(c) => c,
                Err(err) => {
                    tracing::error!("Incoming connection error: {err:?}.");
                    return;
                }
            };
            let Ok(node_id) = connection.remote_node_id() else {
                tracing::error!("Remote node id error");
                return;
            };

            let recv_abort_handle = setup_incoming_listener(
                endpoint.clone(),
                connections.clone(),
                &connection,
                handler.clone(),
            );
            let mut connections = connections.lock().await;
            if let Some(c) = connections.get(&node_id) {
                c.recv_abort_handle.abort();
                c.connection.close(VarInt::from_u32(0), b"disconnected");
            }
            let Some(remote_info) = endpoint.remote_info(node_id) else {
                tracing::error!("Remote info error.");
                return;
            };
            let node_addrs: NodeAddr = remote_info.into();
            let peer_url = match node_addr_to_peer_url(node_addrs.clone()) {
                Ok(u) => u,
                Err(err) => {
                    tracing::error!(
                        "Could not convert remote info to peer url: {err:?}."
                    );
                    return;
                }
            };

            connections.insert(
                node_id.clone(),
                PeerConnection {
                    connection,
                    recv_abort_handle,
                    current_peer_url: peer_url,
                },
            );
        });
    }
}

fn setup_incoming_listener(
    endpoint: Arc<Endpoint>,
    connections: Arc<Mutex<BTreeMap<NodeId, PeerConnection>>>,
    connection: &Connection,
    handler: Arc<TxImpHnd>,
) -> tokio::task::AbortHandle {
    let connection = connection.clone();
    tokio::spawn(async move {
        loop {
            let result = connection.accept_uni().await;
            let Ok(mut recv) = result else {
                tracing::error!("Accept uni error: {result:?}");
                return;
            };

            let Ok(data) = recv.read_to_end(1_000_000_000).await else {
                tracing::error!("Read to end error");
                return;
            };
            let Ok(node_id) = connection.remote_node_id() else {
                tracing::error!("Remote node id error");
                return;
            };

            let connections = connections.lock().await;
            let peer = match connections.get(&node_id) {
                Some(peer_connection) => peer_connection.current_peer_url.clone(),
                None => {
                    let Some(remote_info) = endpoint.remote_info(node_id) else {
                        tracing::error!("Remote info error.");
                        return;
                    };
                    let node_addrs: NodeAddr = remote_info.into();
                    let peer = match node_addr_to_peer_url(node_addrs.clone()) {
                        Ok(u) => u,
                        Err(err) => {
                            tracing::error!(
                                "Could not convert remote info to peer url: {err:?}."
                            );
                            return;
                        }
                    };
                   peer
                }
            };
            drop(connections);

            tracing::debug!("Incoming accept_uni received for {peer}.");

            let Ok(()) = handler.recv_data(peer.clone(), data.into()) else {
                tracing::error!("recv_data error");
                return;
            };

            tracing::debug!("Correctly recv_data for {peer}.");
        }
    })
    .abort_handle()
}
