use super::anychannel::AnyAsyncChannelMap;
use super::anychannel::AnyAsyncSenderErr;
use super::anychannel::AnyUnboundedChannel;
use super::peer_stream::PeerStream;
use super::peer_stream_connect::PeerStreamConnect;
use super::protopack;
use super::protopack::TunnelArcPack;
use super::protopack::TunnelClose;
use super::protopack::TunnelCreateConnect;
use super::protopack::TunnelData;
use super::protopack::TunnelHeaderType;
use super::protopack::TunnelHello;
use super::protopack::TUNNEL_VERSION;
use super::server::AcceptSenderType;
use super::stream::Stream;
use super::stream_flow::StreamFlow;
use super::stream_pack::StreamPack;
use crate::anychannel::AnyUnboundedReceiver;
use anyhow::anyhow;
use anyhow::Result;
use chrono::prelude::*;
use hashbrown::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct PeerClientSender {
    pub peer_client_cmd_tx: mpsc::UnboundedSender<PeerClientCmd>,
    pub stream_pack_to_peer_stream_tx: async_channel::Sender<TunnelArcPack>,
    pub stream_pack_to_peer_stream_rx: async_channel::Receiver<TunnelArcPack>,
    pub peer_stream_to_peer_client_tx:
        mpsc::UnboundedSender<(u32, TunnelHeaderType, TunnelArcPack)>,
    pub peer_stream_len: Arc<AtomicI32>,
}

impl PeerClientSender {
    pub fn new(
        peer_client_cmd_tx: mpsc::UnboundedSender<PeerClientCmd>,
        stream_pack_to_peer_stream_tx: async_channel::Sender<TunnelArcPack>,
        stream_pack_to_peer_stream_rx: async_channel::Receiver<TunnelArcPack>,
        peer_stream_to_peer_client_tx: mpsc::UnboundedSender<(
            u32,
            TunnelHeaderType,
            TunnelArcPack,
        )>,
        peer_stream_len: Arc<AtomicI32>,
    ) -> PeerClientSender {
        PeerClientSender {
            peer_client_cmd_tx,
            stream_pack_to_peer_stream_tx,
            stream_pack_to_peer_stream_rx,
            peer_stream_to_peer_client_tx,
            peer_stream_len,
        }
    }
}

pub enum PeerClientCmd {
    RegisterStream(
        (
            u32,
            mpsc::UnboundedSender<(AnyUnboundedReceiver<TunnelArcPack>, SocketAddr, SocketAddr)>,
        ),
    ),
    PackWaiting((u32, u32)),
    PeerStreamFlow((i64, i64)),
}

pub struct PeerClient {
    session_id: String,
    peer_stream_max_len: Arc<AtomicUsize>,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    accept_tx: Option<AcceptSenderType>,
    peer_stream_connect: Option<(Arc<Box<dyn PeerStreamConnect>>, SocketAddr)>,
    stream_pack_tx_map: AnyAsyncChannelMap<u32, AnyUnboundedChannel<TunnelArcPack>>,
    waiting_pack_len_map: HashMap<u32, u32>,
    peer_client_cmd_rx: mpsc::UnboundedReceiver<PeerClientCmd>,
    peer_stream_to_peer_client_rx: mpsc::UnboundedReceiver<(u32, TunnelHeaderType, TunnelArcPack)>,
    peer_client_sender: PeerClientSender,
    last_check_connect_timestamp: i64,
    flow_read: i64,
    flow_write: i64,
}

impl PeerClient {
    pub fn new(
        session_id: String,
        peer_stream_max_len: Arc<AtomicUsize>,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        accept_tx: Option<AcceptSenderType>,
        peer_stream_connect: Option<(Arc<Box<dyn PeerStreamConnect>>, SocketAddr)>,
    ) -> PeerClient {
        let (peer_client_cmd_tx, peer_client_cmd_rx) = mpsc::unbounded_channel();
        let (peer_stream_to_peer_client_tx, peer_stream_to_peer_client_rx) =
            mpsc::unbounded_channel();
        let (stream_pack_to_peer_stream_tx, stream_pack_to_peer_stream_rx) =
            async_channel::bounded(200);

        let peer_stream_len = Arc::new(AtomicI32::new(0));
        PeerClient {
            session_id,
            peer_stream_max_len,
            local_addr,
            remote_addr,
            accept_tx,
            peer_stream_connect,
            stream_pack_tx_map: AnyAsyncChannelMap::<u32, AnyUnboundedChannel<TunnelArcPack>>::new(
            ),
            waiting_pack_len_map: HashMap::new(),
            peer_client_sender: PeerClientSender::new(
                peer_client_cmd_tx,
                stream_pack_to_peer_stream_tx,
                stream_pack_to_peer_stream_rx,
                peer_stream_to_peer_client_tx,
                peer_stream_len,
            ),
            peer_client_cmd_rx,
            peer_stream_to_peer_client_rx,
            last_check_connect_timestamp: Local::now().timestamp(),
            flow_read: 0,
            flow_write: 0,
        }
    }

    pub fn get_peer_client_sender(&self) -> PeerClientSender {
        self.peer_client_sender.clone()
    }

    pub async fn start_cmd(&mut self) -> Result<()> {
        pub enum PeerClientMsgCmd {
            Cmd(Option<PeerClientCmd>),
            PeerStreamToPeerClient(Option<(u32, TunnelHeaderType, TunnelArcPack)>),
            Timeout(),
        }

        loop {
            let ret: Result<()> = async {
                let msg: Result<PeerClientMsgCmd> = async {
                    tokio::select! {
                        biased;
                        msg = self.peer_client_cmd_rx.recv() => {
                            Ok(PeerClientMsgCmd::Cmd(msg))
                        }
                        msg = self.peer_stream_to_peer_client_rx.recv() => {
                            Ok(PeerClientMsgCmd::PeerStreamToPeerClient(msg))
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                            Ok(PeerClientMsgCmd::Timeout())
                        }
                        else => {
                            return Err(anyhow!("err:start_cmd"))?;
                        }
                    }
                }
                .await;
                let msg = msg?;
                match msg {
                    PeerClientMsgCmd::Cmd(msg) => {
                        self.cmd_cmd(msg)
                            .await
                            .map_err(|e| anyhow!("err:cmd_cmd => e:{}", e))?;
                    }
                    PeerClientMsgCmd::PeerStreamToPeerClient(msg) => {
                        self.cmd_peer_stream_to_peer_client(msg)
                            .await
                            .map_err(|e| {
                                anyhow!("err:cmd_peer_stream_to_peer_client => e:{}", e)
                            })?;
                    }
                    PeerClientMsgCmd::Timeout() => {}
                }

                let check_connet_interval = 3;
                let curr_timestamp = Local::now().timestamp();
                if curr_timestamp - self.last_check_connect_timestamp
                    >= check_connet_interval as i64
                {
                    self.last_check_connect_timestamp = curr_timestamp;
                    self.cmd_check_connect()
                        .await
                        .map_err(|e| anyhow!("err:cmd_check_connect => e:{}", e))?;
                }
                Ok(())
            }
            .await;
            ret.unwrap_or_else(|e| log::error!("err:start_cmd => e:{}", e));
        }
    }

    pub async fn cmd_cmd(&mut self, msg: Option<PeerClientCmd>) -> Result<()> {
        match msg {
            Some(cmd) => match cmd {
                PeerClientCmd::RegisterStream(cmd) => self.cmd_register_stream(cmd).await,
                PeerClientCmd::PackWaiting(cmd) => self.cmd_pack_waiting(cmd).await,
                PeerClientCmd::PeerStreamFlow(cmd) => self.cmd_peer_stream_flow(cmd).await,
            },
            None => Ok(()),
        }
    }

    pub async fn cmd_peer_stream_to_peer_client(
        &mut self,
        msg: Option<(u32, TunnelHeaderType, TunnelArcPack)>,
    ) -> Result<()> {
        if msg.is_none() {
            return Ok(());
        }

        let (stream_id, typ, pack) = msg.unwrap();
        if typ == TunnelHeaderType::TunnelCreateConnect {
            let connect_count = self
                .peer_client_sender
                .peer_stream_len
                .load(Ordering::Relaxed);
            if self.peer_stream_connect.is_some()
                && connect_count < self.peer_stream_max_len.load(Ordering::Relaxed) as i32
            {
                log::debug!(
                    "connect_count:{}, peer_stream_max_len:{}",
                    connect_count,
                    self.peer_stream_max_len.load(Ordering::Relaxed)
                );
                let _ = self.create_connect().await;
            }
            return Ok(());
        }
        let ret = self.stream_pack_tx_map.send(stream_id, pack).await;
        match ret {
            AnyAsyncSenderErr::Err(e) => {
                log::debug!("stream_pack_tx_map:{}", e);
                return Ok(());
            }
            AnyAsyncSenderErr::Ok => {
                return Ok(());
            }
            AnyAsyncSenderErr::Close(_) => {}
            AnyAsyncSenderErr::None(pack) => {
                if self.accept_tx.is_some() {
                    log::debug!("accept_tx send");
                    let stream = self
                        .register_stream(stream_id)
                        .await
                        .map_err(|e| anyhow!("err:register_stream => e:{}", e))?;
                    self.accept_tx
                        .as_ref()
                        .unwrap()
                        .send(stream)
                        .await
                        .map_err(|_| anyhow!("err:accept_tx"))?;
                    self.stream_pack_tx_map.send(stream_id, pack).await;
                    return Ok(());
                }
            }
        }

        if typ != TunnelHeaderType::TunnelClose {
            let peer_client_sender = &self.peer_client_sender;
            let close_pack = Arc::new(TunnelClose { stream_id });
            peer_client_sender
                .stream_pack_to_peer_stream_tx
                .send(TunnelArcPack::TunnelClose(close_pack))
                .await?;
        }
        Ok(())
    }

    pub async fn cmd_check_connect(&mut self) -> Result<()> {
        // let flow_read = self.flow_read;
        // let flow_write = self.flow_write;
        self.flow_read = 0;
        self.flow_write = 0;

        // log::info!(
        //     "flow_read:{:.3}M, flow_write:{:.3}M",
        //     flow_read as f64 / 1024 as f64 / 1024 as f64,
        //     flow_write as f64 / 1024 as f64 / 1024 as f64
        // );

        let connect_count = self
            .peer_client_sender
            .peer_stream_len
            .load(Ordering::Relaxed);

        let mut is_register_peer_stream = true;
        #[cfg(feature = "tunnel-wait-ack")]
        {
            let waiting_pack_len = self
                .waiting_pack_len_map
                .iter()
                .fold(0, |waiting_pack_len, (_, value)| waiting_pack_len + value)
                as usize;
            log::debug!("waiting_pack_len:{}", waiting_pack_len);
            if (waiting_pack_len as i32) < connect_count * 2 {
                is_register_peer_stream = false;
            }
        }

        if connect_count <= 0 || is_register_peer_stream {
            log::debug!(
                "connect_count:{}, peer_stream_max_len:{}",
                connect_count,
                self.peer_stream_max_len.load(Ordering::Relaxed),
            );

            if self.peer_stream_connect.is_none() {
                let peer_client_sender = &self.peer_client_sender;
                let create_connect = Arc::new(TunnelCreateConnect { stream_id: 0 });

                //log::debug!("send create_connect");
                peer_client_sender
                    .stream_pack_to_peer_stream_tx
                    .send(TunnelArcPack::TunnelCreateConnect(create_connect))
                    .await?;
            } else {
                if connect_count < self.peer_stream_max_len.load(Ordering::Relaxed) as i32 {
                    let _ = self.create_connect().await;
                }
            }
        }

        Ok(())
    }

    pub async fn create_connect(&mut self) -> Result<()> {
        let (peer_stream_connect, connect_addr) = self.peer_stream_connect.as_ref().unwrap();

        log::debug!("connect_addr:{}", connect_addr);
        let (stream, _, _) = peer_stream_connect
            .connect(connect_addr)
            .await
            .map_err(|e| anyhow!("err:peer_stream_connect.connect => e:{}", e))?;
        let session_id = self.session_id.clone();
        PeerClient::client_insert_peer_stream(true, &self.peer_client_sender, session_id, stream)
            .await
            .map_err(|e| anyhow!("err:client_insert_peer_stream => e:{}", e))?;
        Ok(())
    }

    pub async fn cmd_register_stream(
        &mut self,
        cmd: (
            u32,
            mpsc::UnboundedSender<(AnyUnboundedReceiver<TunnelArcPack>, SocketAddr, SocketAddr)>,
        ),
    ) -> Result<()> {
        let (stream_id, tx) = cmd;
        let peer_client_to_stream_pack_rx = self.stream_pack_tx_map.channel(0, stream_id);
        tx.send((
            peer_client_to_stream_pack_rx,
            self.local_addr.clone(),
            self.remote_addr.clone(),
        ))
        .map_err(|_| anyhow!("err:cmd_register_stream"))?;
        Ok(())
    }

    pub async fn cmd_pack_waiting(&mut self, cmd: (u32, u32)) -> Result<()> {
        let (stream_id, waiting_pack_len) = cmd;
        self.waiting_pack_len_map
            .insert(stream_id, waiting_pack_len);
        Ok(())
    }

    pub async fn cmd_peer_stream_flow(&mut self, cmd: (i64, i64)) -> Result<()> {
        let (read, write) = cmd;
        self.flow_read += read;
        self.flow_write += write;
        Ok(())
    }

    pub async fn client_insert_peer_stream(
        is_spawn: bool,
        peer_client_sender: &PeerClientSender,
        session_id: String,
        mut stream: StreamFlow,
    ) -> Result<()> {
        let hello_pack = TunnelHello {
            version: TUNNEL_VERSION.to_string(),
            session_id,
        };
        log::debug!("client hello_pack:{:?}", hello_pack);
        let (_, mut w) = tokio::io::split(&mut stream);
        protopack::write_pack(
            &mut w,
            protopack::TunnelHeaderType::TunnelHello,
            &hello_pack,
            true,
        )
        .await
        .map_err(|e| anyhow!("err:protopack::write_pack => e:{}", e))?;

        PeerClient::insert_peer_stream(is_spawn, peer_client_sender, stream)
            .await
            .map_err(|e| anyhow!("err:insert_peer_stream => e:{}", e))?;
        Ok(())
    }

    pub async fn insert_peer_stream(
        is_spawn: bool,
        peer_client_sender: &PeerClientSender,
        stream: StreamFlow,
    ) -> Result<()> {
        let peer_client_cmd_tx = peer_client_sender.peer_client_cmd_tx.clone();
        let peer_stream_to_peer_client_tx =
            peer_client_sender.peer_stream_to_peer_client_tx.clone();
        let peer_stream_len = peer_client_sender.peer_stream_len.clone();
        let stream_pack_to_peer_stream_rx =
            peer_client_sender.stream_pack_to_peer_stream_rx.clone();
        PeerStream::start(
            peer_stream_len,
            is_spawn,
            stream,
            peer_client_cmd_tx,
            peer_stream_to_peer_client_tx,
            stream_pack_to_peer_stream_rx,
        )
        .await
        .map_err(|e| anyhow!("err:PeerStream::start => e:{}", e))?;
        Ok(())
    }

    pub async fn async_register_stream(
        peer_client_sender: &PeerClientSender,
        stream_id: u32,
    ) -> Result<(Stream, SocketAddr, SocketAddr)> {
        let (wait_tx, mut wait_rx) = mpsc::unbounded_channel();
        peer_client_sender
            .peer_client_cmd_tx
            .send(PeerClientCmd::RegisterStream((stream_id, wait_tx)))
            .map_err(|_| anyhow!("err:RegisterStream"))?;
        let msg = wait_rx.recv().await;
        if msg.is_none() {
            return Err(anyhow!("err:RegisterStream"))?;
        }
        let (peer_client_to_stream_pack_rx, local_addr, remote_addr) = msg.unwrap();
        let peer_client_cmd_tx = peer_client_sender.peer_client_cmd_tx.clone();
        let stream_pack_to_peer_stream_tx =
            peer_client_sender.stream_pack_to_peer_stream_tx.clone();
        PeerClient::insert_stream(
            peer_client_cmd_tx,
            stream_pack_to_peer_stream_tx,
            stream_id,
            local_addr,
            remote_addr,
            peer_client_to_stream_pack_rx,
        )
        .await
    }

    pub async fn register_stream(
        &mut self,
        stream_id: u32,
    ) -> Result<(Stream, SocketAddr, SocketAddr)> {
        log::debug!("accept_map stream_id:{:?}", stream_id);
        let peer_client_sender = &self.peer_client_sender;
        let local_addr = self.local_addr.clone();
        let remote_addr = self.remote_addr.clone();
        let peer_client_to_stream_pack_rx = self.stream_pack_tx_map.channel(0, stream_id);
        let peer_client_cmd_tx = peer_client_sender.peer_client_cmd_tx.clone();
        let stream_pack_to_peer_stream_tx =
            peer_client_sender.stream_pack_to_peer_stream_tx.clone();
        PeerClient::insert_stream(
            peer_client_cmd_tx,
            stream_pack_to_peer_stream_tx,
            stream_id,
            local_addr,
            remote_addr,
            peer_client_to_stream_pack_rx,
        )
        .await
    }

    pub async fn insert_stream(
        peer_client_cmd_tx: mpsc::UnboundedSender<PeerClientCmd>,
        stream_pack_to_peer_stream_tx: async_channel::Sender<TunnelArcPack>,
        stream_id: u32,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        peer_client_to_stream_pack_rx: AnyUnboundedReceiver<TunnelArcPack>,
    ) -> Result<(Stream, SocketAddr, SocketAddr)> {
        let (stream_to_stream_pack_tx, stream_to_stream_pack_rx) = async_channel::bounded(100);
        let (stream_pack_to_stream_tx, stream_pack_to_stream_rx) =
            mpsc::channel::<Arc<TunnelData>>(100);

        tokio::spawn(async move {
            let stream_pack = StreamPack::new(stream_id, stream_pack_to_peer_stream_tx);
            if let Err(e) = stream_pack
                .start(
                    stream_pack_to_stream_tx,
                    peer_client_to_stream_pack_rx,
                    stream_to_stream_pack_rx,
                    peer_client_cmd_tx,
                )
                .await
            {
                log::error!("err:pack.start() => e:{}", e);
            }
        });

        Ok((
            Stream::new(stream_to_stream_pack_tx, stream_pack_to_stream_rx),
            local_addr,
            remote_addr,
        ))
    }
}
