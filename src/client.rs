use super::peer_client::PeerClient;
use super::peer_client::PeerClientSender;
use super::peer_stream_connect::PeerStreamConnect;
use super::stream::Stream;
use super::tunnel::Tunnel;
use super::Protocol4;
use chrono::prelude::*;
use lazy_static::lazy_static;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

lazy_static! {
    static ref CLIENT_ID: AtomicU32 = AtomicU32::new(1);
    static ref STREAM_ID: AtomicU32 = AtomicU32::new(1);
}

pub struct ClientContext {
    tunnel: Tunnel,
    pid: i32,
    client_id: u32,
    peer_stream_max_len: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct Client {
    context: Arc<Mutex<ClientContext>>,
}

impl Client {
    pub fn new(tunnel: Tunnel, peer_stream_max_len: Arc<AtomicUsize>) -> Client {
        let pid = unsafe { libc::getpid() };
        let client_id = CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        Client {
            context: Arc::new(Mutex::new(ClientContext {
                tunnel,
                pid,
                client_id,
                peer_stream_max_len,
            })),
        }
    }

    pub fn register_id(proto: &Protocol4, sock_addr: &SocketAddr) -> String {
        proto.to_string() + &sock_addr.to_string()
    }

    pub async fn get_peer_client(&self, register_id: &String) -> Option<PeerClientSender> {
        let context = self.context.lock().await;
        let tunnel_key = context.client_id.to_string();
        context
            .tunnel
            .get_peer_client(&tunnel_key, register_id)
            .await
    }

    pub async fn insert_peer_client(
        &self,
        register_id: String,
        session_id: String,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        peer_stream_connect: (Arc<Box<dyn PeerStreamConnect>>, SocketAddr),
    ) -> anyhow::Result<PeerClientSender> {
        let context = self.context.lock().await;
        let tunnel_key = context.client_id.to_string();
        let peer_stream_max_len = context.peer_stream_max_len.clone();
        let peer_client_sender = context
            .tunnel
            .insert_peer_client(
                tunnel_key,
                register_id,
                session_id,
                local_addr,
                remote_addr,
                peer_stream_max_len,
                None,
                Some(peer_stream_connect),
            )
            .await?;
        Ok(peer_client_sender)
    }

    pub async fn connect(
        &self,
        peer_stream_connect: Arc<Box<dyn PeerStreamConnect>>,
    ) -> anyhow::Result<(Stream, SocketAddr, SocketAddr)> {
        let connect_addr = peer_stream_connect.connect_addr().await?;
        let proto = peer_stream_connect.protocol4().await;
        let register_id = Client::register_id(&proto, &connect_addr);
        let peer_client_sender = {
            let peer_client_sender = self.get_peer_client(&register_id).await;
            if peer_client_sender.is_some() {
                peer_client_sender.unwrap()
            } else {
                let (stream, local_addr, remote_addr) =
                    peer_stream_connect.connect(&connect_addr).await?;
                let session_id = {
                    let context = self.context.lock().await;
                    format!(
                        "{}{:?}{}{}{}{}",
                        context.pid,
                        std::thread::current().id(),
                        context.client_id,
                        local_addr,
                        remote_addr,
                        Local::now().timestamp_millis(),
                    )
                };
                let peer_client_sender = self
                    .insert_peer_client(
                        register_id,
                        session_id.clone(),
                        local_addr,
                        remote_addr,
                        (peer_stream_connect, connect_addr),
                    )
                    .await?;
                PeerClient::client_insert_peer_stream(
                    true,
                    &peer_client_sender,
                    session_id,
                    stream,
                )
                .await?;

                peer_client_sender
            }
        };
        let stream_id = STREAM_ID.fetch_add(1, Ordering::Relaxed);
        let stream = PeerClient::async_register_stream(&peer_client_sender, stream_id).await?;
        Ok(stream)
    }
}
