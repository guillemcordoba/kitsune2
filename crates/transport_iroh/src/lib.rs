#![deny(missing_docs)]
//! kitsune2 iroh transport module.

use base64::Engine;
use iroh::{
    endpoint::{Connection, VarInt},
    Endpoint, NodeAddr, NodeId, RelayMap, RelayMode, RelayUrl,
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
    node_addr: NodeAddr,
    connection: Connection,
    recv_abort_handle: AbortHandle,
}

struct IrohTransport {
    endpoint: Arc<Endpoint>,
    connections: Arc<Mutex<BTreeMap<NodeAddr, PeerConnection>>>,
    evt_task: tokio::task::AbortHandle,
    handler: Arc<TxImpHnd>,
}

impl IrohTransport {
    async fn get_or_open_connection_with(
        &self,
        node_addr: &NodeAddr,
    ) -> Result<PeerConnection, K2Error> {
        let mut connections = self.connections.lock().await;

        if let Some(connection) = connections.get(&node_addr) {
            Ok(connection.clone())
        } else {
            let connection = self
                .endpoint
                .connect(node_addr.clone(), ALPN)
                .await
                .map_err(|err| {
                    K2Error::other(format!("failed to connect: {err:?}"))
                })?;
            let recv_abort_handle = setup_incoming_listener(
                self.endpoint.clone(),
                &connection,
                self.handler.clone(),
            );
            let peer_connection = PeerConnection {
                node_addr: node_addr.clone(),
                connection: connection.clone(),
                recv_abort_handle,
            };
            connections.insert(node_addr.clone(), peer_connection.clone());
            Ok(peer_connection)
        }
    }
}

fn setup_incoming_listener(
    endpoint: Arc<Endpoint>,
    connection: &Connection,
    handler: Arc<TxImpHnd>,
) -> tokio::task::AbortHandle {
    let connection = connection.clone();
    tokio::spawn(async move {
        loop {
            let Ok(mut recv) = connection.accept_uni().await else {
                tracing::error!("Accept uni error");
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

            let Some(remote_info) = endpoint.remote_info(node_id) else {
                tracing::error!("Remote info error ");
                return;
            };
            let Some(relay_url_info) = remote_info.relay_url else {
                tracing::error!("Remote info error ");
                return;
            };

            let Ok(peer) =
                to_peer_url(relay_url_info.relay_url.into(), node_id)
            else {
                tracing::error!("Url from str error");
                return;
            };

            let Ok(()) = handler.recv_data(peer, data.into()) else {
                tracing::error!("recv_data error");
                return;
            };
        }
    })
    .abort_handle()
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
        self.evt_task.abort();
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
                        K2Error::other("Failed to parse custom relay url")
                    })?;
                RelayMode::Custom(RelayMap::from_url(relay_url.into()))
            }
            None => RelayMode::Default,
        };
        let endpoint = iroh::Endpoint::builder()
            .relay_mode(relay_mode)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .map_err(|err| K2Error::other("bad"))?;

        let _relay_url = endpoint.home_relay().initialized().await.unwrap();
        let endpoint = Arc::new(endpoint);

        let connections = Arc::new(Mutex::new(BTreeMap::new()));

        let evt_task = tokio::task::spawn(evt_task(
            connections.clone(),
            handler.clone(),
            endpoint.clone(),
        ))
        .abort_handle();

        let out: DynTxImp = Arc::new(Self {
            endpoint,
            connections,
            evt_task,
            handler,
        });

        Ok(out)
    }
}

fn peer_url_to_node_addr(peer_url: Url) -> Result<NodeAddr, K2Error> {
    let url = url::Url::parse(peer_url.as_str()).map_err(|err| {
        K2Error::other(format!("Failed to parse peer url: {err:?}"))
    })?;
    let Some(peer_id) = peer_url.peer_id() else {
        return Err(K2Error::other("empty peer url"));
    };
    let decoded_peer_id = base64::prelude::BASE64_URL_SAFE_NO_PAD
        .decode(peer_id)
        .map_err(|err| K2Error::other("failed to decode peer id"))?;
    let node_id = NodeId::try_from(decoded_peer_id.as_slice())
        .map_err(|err| K2Error::other(format!("bad peer id: {err}")))?;

    let relay_url = url::Url::parse(
        format!("{}://{}", url.scheme(), peer_url.addr()).as_str(),
    )
    .map_err(|err| K2Error::other("Bad addr"))?;

    Ok(NodeAddr {
        node_id,
        relay_url: Some(RelayUrl::from(relay_url)),
        direct_addresses: BTreeSet::new(),
    })
}

fn to_peer_url(url: url::Url, node_id: NodeId) -> Result<Url, K2Error> {
    let port = url.port().unwrap_or(443);

    let mut url_str = url.to_string();
    if let Some(s) = url_str.strip_suffix("./") {
        url_str = s.to_string();
    }
    let u = format!(
        "{}:{port}/{}",
        url_str,
        base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(node_id)
    );
    Url::from_str(u.as_str())
}

fn node_addr_to_peer_url(node_addr: NodeAddr) -> Result<Url, K2Error> {
    match node_addr.relay_url {
        Some(relay_url) => to_peer_url(relay_url.into(), node_addr.node_id),
        None => {
            let Some(direct_address) = node_addr
                .direct_addresses
                .into_iter()
                .collect::<Vec<SocketAddr>>()
                .first()
                .cloned()
            else {
                return Err(K2Error::other(
                    "node addr has no relay url and no direct addresses",
                ));
            };
            let Ok(url) =
                url::Url::parse(format!("http://{}", direct_address).as_str())
            else {
                return Err(K2Error::other("Failed to parse direct addresses"));
            };
            to_peer_url(url, node_addr.node_id)
        }
    }
}

impl TxImp for IrohTransport {
    fn url(&self) -> Option<Url> {
        let home_relay = self.endpoint.home_relay().get();
        let Ok(Some(url)) = home_relay else {
            tracing::error!("Failed to get home relay");
            return None;
        };
        Some(
            to_peer_url(url.into(), self.endpoint.node_id())
                .expect("Invalid URL"),
        )
    }

    fn disconnect(
        &self,
        peer: Url,
        _payload: Option<(String, bytes::Bytes)>,
    ) -> BoxFut<'_, ()> {
        Box::pin(async move {
            let Ok(addr) = peer_url_to_node_addr(peer) else {
                tracing::error!("Bad peer url to node addr");
                return;
            };
            let mut connections = self.connections.lock().await;
            if let Some(peer_connection) = connections.get(&addr) {
                peer_connection
                    .connection
                    .close(VarInt::from_u32(0), b"disconnected");
                peer_connection.recv_abort_handle.abort();
                connections.remove(&addr);
            }
            ()
        })
    }

    fn send(&self, peer: Url, data: bytes::Bytes) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            let addr = peer_url_to_node_addr(peer).map_err(|err| {
                K2Error::other(format!("bad peer url: {:?}", err))
            })?;

            let peer_connection =
                self.get_or_open_connection_with(&addr).await?;

            let mut send = match peer_connection.connection.open_uni().await {
                Ok(s) => s,
                Err(_) => {
                    let mut connections = self.connections.lock().await;
                    peer_connection.recv_abort_handle.abort();
                    peer_connection
                        .connection
                        .close(VarInt::from_u32(0), b"disconnected");
                    connections.remove(&addr);

                    let new_peer_connection =
                        self.get_or_open_connection_with(&addr).await?;
                    new_peer_connection
                        .connection
                        .open_uni()
                        .await
                        .map_err(|err| K2Error::other("could not open uni"))?
                }
            };
            // .map_err(|err| K2Error::other("Failed to open uni strem"))?;

            send.write_all(data.as_ref())
                .await
                .map_err(|err| K2Error::other("Failed to write all"))?;
            send.finish()
                .map_err(|err| K2Error::other("Failed to close stream"))?;
            send.stopped()
                .await
                .map_err(|err| K2Error::other("error stopping"))?;
            // connection.closed().await;
            Ok(())
        })
    }

    fn dump_network_stats(&self) -> BoxFut<'_, K2Result<TransportStats>> {
        Box::pin(async move {
            let connections = self.connections.lock().await;

            Ok(TransportStats {
                backend: format!("iroh"),
                peer_urls: connections
                    .iter()
                    .filter_map(|(node_addr, _)| {
                        node_addr_to_peer_url(node_addr.clone()).ok()
                    })
                    .collect(),
                connections: connections
                    .iter()
                    .map(|(peer_addr, conn)| TransportConnectionStats {
                        pub_key: base64::prelude::BASE64_URL_SAFE_NO_PAD
                            .encode(peer_addr.node_id),
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
}

async fn evt_task(
    connections: Arc<Mutex<BTreeMap<NodeAddr, PeerConnection>>>,
    handler: Arc<TxImpHnd>,
    endpoint: Arc<Endpoint>,
) {
    while let Some(incoming) = endpoint.accept().await {
        let endpoint = endpoint.clone();
        let handler = handler.clone();
        let connections = connections.clone();
        tokio::spawn(async move {
            let Ok(connection) = incoming.await else {
                tracing::error!("Incoming connection error");
                return;
            };
            let Ok(node_id) = connection.remote_node_id() else {
                tracing::error!("Remote node id error");
                return;
            };

            let Some(remote_info) = endpoint.remote_info(node_id) else {
                tracing::error!("Remote info error ");
                return;
            };
            let node_addr: NodeAddr = remote_info.into();
            let recv_abort_handle =
                setup_incoming_listener(endpoint, &connection, handler.clone());
            let mut connections = connections.lock().await;
            if let Some(c) = connections.get(&node_addr) {
                c.recv_abort_handle.abort();
                c.connection.close(VarInt::from_u32(0), b"disconnected");
            }

            connections.insert(
                node_addr.clone(),
                PeerConnection {
                    node_addr,
                    connection,
                    recv_abort_handle,
                },
            );
        });
    }
}
