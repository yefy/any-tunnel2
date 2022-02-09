use super::stream_flow::StreamFlow;
use super::Protocol4;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use tokio::net::TcpStream;

#[async_trait]
pub trait PeerStreamConnect: Send + Sync {
    async fn connect(&self, _: &SocketAddr)
        -> anyhow::Result<(StreamFlow, SocketAddr, SocketAddr)>;
    async fn connect_addr(&self) -> anyhow::Result<SocketAddr>; //这个是域名获取到的ip地址
    async fn address(&self) -> String; //地址或域名  配上文件上的
    async fn protocol4(&self) -> Protocol4; //tcp 或 udp  四层协议
    async fn protocol7(&self) -> String; //tcp udp  quic http 等7层协议
}

pub struct PeerStreamConnectTcp {
    addr: String,
}

impl PeerStreamConnectTcp {
    pub fn new(addr: String) -> PeerStreamConnectTcp {
        PeerStreamConnectTcp { addr }
    }
}

#[async_trait]
impl PeerStreamConnect for PeerStreamConnectTcp {
    async fn connect(
        &self,
        connect_addr: &SocketAddr,
    ) -> anyhow::Result<(StreamFlow, SocketAddr, SocketAddr)> {
        let stream = TcpStream::connect(connect_addr).await?;
        let local_addr = stream.local_addr()?;
        let remote_addr = stream.peer_addr()?;
        Ok((StreamFlow::new(Box::new(stream)), local_addr, remote_addr))
    }
    async fn connect_addr(&self) -> anyhow::Result<SocketAddr> {
        let connect_addr =
            self.addr.to_socket_addrs()?.next().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::Other, "err:empty address")
            })?;
        Ok(connect_addr)
    }

    async fn address(&self) -> String {
        self.addr.clone()
    }

    async fn protocol4(&self) -> Protocol4 {
        Protocol4::TCP
    }
    async fn protocol7(&self) -> String {
        "tcp".to_string()
    }
}
