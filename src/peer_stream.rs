use super::peer_client::PeerClientCmd;
use super::protopack;
use super::protopack::TunnelArcPack;
use super::protopack::TunnelHeaderType;
use super::protopack::TunnelPack;
use super::stream_flow::StreamFlow;
use super::stream_flow::StreamFlowErr;
use super::stream_flow::StreamFlowInfo;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::io::BufWriter;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

pub struct PeerStream {}

impl PeerStream {
    pub async fn start(
        peer_stream_len: Arc<AtomicI32>,
        is_spawn: bool,
        stream: StreamFlow,
        peer_client_cmd_tx: mpsc::UnboundedSender<PeerClientCmd>,
        peer_stream_to_peer_client_tx: mpsc::UnboundedSender<(
            u32,
            TunnelHeaderType,
            TunnelArcPack,
        )>,
        pack_to_peer_stream_rx: async_channel::Receiver<TunnelArcPack>,
    ) -> anyhow::Result<()> {
        if is_spawn {
            tokio::spawn(async move {
                let ret: anyhow::Result<()> = async {
                    PeerStream::do_start(
                        peer_stream_len,
                        stream,
                        peer_client_cmd_tx,
                        peer_stream_to_peer_client_tx,
                        pack_to_peer_stream_rx,
                    )
                    .await?;
                    Ok(())
                }
                .await;
                ret.unwrap_or_else(|e| log::error!("err:PeerStream::do_start => e:{}", e));
            });
        } else {
            PeerStream::do_start(
                peer_stream_len,
                stream,
                peer_client_cmd_tx,
                peer_stream_to_peer_client_tx,
                pack_to_peer_stream_rx,
            )
            .await
            .map_err(|e| anyhow::anyhow!("err:PeerStream::do_start => e:{}", e))?;
        }
        Ok(())
    }

    pub async fn do_start(
        peer_stream_len: Arc<AtomicI32>,
        mut stream: StreamFlow,
        peer_client_cmd_tx: mpsc::UnboundedSender<PeerClientCmd>,
        peer_stream_to_peer_client_tx: mpsc::UnboundedSender<(
            u32,
            TunnelHeaderType,
            TunnelArcPack,
        )>,
        mut pack_to_peer_stream_rx: async_channel::Receiver<TunnelArcPack>,
    ) -> anyhow::Result<()> {
        peer_stream_len.fetch_add(1, Ordering::Relaxed);
        let stream_info = Arc::new(std::sync::Mutex::new(StreamFlowInfo::new()));
        stream.set_config(
            tokio::time::Duration::from_secs(60 * 10),
            tokio::time::Duration::from_secs(60 * 10),
            true,
            Some(stream_info.clone()),
        );
        let (r, w) = tokio::io::split(stream);

        let ret: anyhow::Result<()> = async {
            tokio::select! {
                biased;
                ret = PeerStream::read_stream(r,  &peer_stream_to_peer_client_tx) => {
                    ret.map_err(|e| anyhow::anyhow!("err:peer_stream read_stream => e:{}", e))?;
                    Ok(())
                }
                ret = PeerStream::write_stream(w, &mut pack_to_peer_stream_rx) => {
                    ret.map_err(|e| anyhow::anyhow!("err:peer_stream write_stream => e:{}", e))?;
                    Ok(())
                }
                ret = PeerStream::stream_flow(stream_info.clone(), peer_client_cmd_tx) => {
                    ret.map_err(|e| anyhow::anyhow!("err:peer_stream stream_flow => e:{}", e))?;
                    Ok(())
                }
                else => {
                    return Err(anyhow::anyhow!("err:upstream_pack.rs"));
                }
            }
        }
        .await;

        peer_stream_len.fetch_sub(1, Ordering::Relaxed);
        if let Err(e) = ret {
            let err = { stream_info.lock().unwrap().err.clone() };
            if err as i32 >= StreamFlowErr::WriteTimeout as i32 {
                return Err(e)?;
            }
        }
        Ok(())
    }

    async fn read_stream<R: AsyncRead + std::marker::Unpin>(
        r: R,
        peer_stream_to_peer_client_tx: &mpsc::UnboundedSender<(
            u32,
            TunnelHeaderType,
            TunnelArcPack,
        )>,
    ) -> anyhow::Result<()> {
        let mut buf_read = BufReader::new(r);
        let mut slice = [0u8; protopack::TUNNEL_MAX_HEADER_SIZE];
        loop {
            let pack = protopack::read_pack(&mut buf_read, &mut slice).await?;
            match pack {
                TunnelPack::TunnelHello(value) => {
                    log::error!("err:TunnelHello => TunnelHello:{:?}", value);
                }
                TunnelPack::TunnelData(value) => {
                    log::trace!("peer_stream_to_peer_client TunnelData:{:?}", value.header);
                    peer_stream_to_peer_client_tx.send((
                        value.header.stream_id,
                        TunnelHeaderType::TunnelData,
                        TunnelArcPack::TunnelData(Arc::new(value)),
                    ))?;
                }
                TunnelPack::TunnelDataAck(value) => {
                    log::trace!("peer_stream_to_peer_client TunnelDataAck:{:?}", value);

                    peer_stream_to_peer_client_tx.send((
                        value.header.stream_id,
                        TunnelHeaderType::TunnelDataAck,
                        TunnelArcPack::TunnelDataAck(Arc::new(value)),
                    ))?;
                }
                TunnelPack::TunnelClose(value) => {
                    log::trace!("peer_stream_to_peer_client TunnelClose:{:?}", value);
                    peer_stream_to_peer_client_tx.send((
                        value.stream_id,
                        TunnelHeaderType::TunnelClose,
                        TunnelArcPack::TunnelClose(Arc::new(value)),
                    ))?;
                }
                TunnelPack::TunnelHeartbeat(value) => {
                    log::trace!("peer_stream_to_peer_client TunnelHeartbeat:{:?}", value);
                    peer_stream_to_peer_client_tx.send((
                        value.stream_id,
                        TunnelHeaderType::TunnelHeartbeat,
                        TunnelArcPack::TunnelHeartbeat(Arc::new(value)),
                    ))?;
                }
                TunnelPack::TunnelHeartbeatAck(value) => {
                    log::trace!("peer_stream_to_peer_client TunnelHeartbeatAck:{:?}", value);
                    peer_stream_to_peer_client_tx.send((
                        value.stream_id,
                        TunnelHeaderType::TunnelHeartbeatAck,
                        TunnelArcPack::TunnelHeartbeatAck(Arc::new(value)),
                    ))?;
                }
                TunnelPack::TunnelCreateConnect(value) => {
                    log::trace!("peer_stream_to_peer_client TunnelCreateConnect:{:?}", value);
                    peer_stream_to_peer_client_tx.send((
                        value.stream_id,
                        TunnelHeaderType::TunnelCreateConnect,
                        TunnelArcPack::TunnelCreateConnect(Arc::new(value)),
                    ))?;
                }
            }
        }
    }

    async fn write_stream<W: AsyncWrite + std::marker::Unpin>(
        w: W,
        pack_to_peer_stream_rx: &mut async_channel::Receiver<TunnelArcPack>,
    ) -> anyhow::Result<()> {
        let mut buf_writer = BufWriter::new(w);

        loop {
            let pack = pack_to_peer_stream_rx
                .recv()
                .await
                .map_err(|_| anyhow::anyhow!("pack_to_peer_stream_rx close"));
            if pack.is_err() {
                log::debug!("pack_to_peer_stream_rx close");
                return Ok(());
            }
            let pack = pack.unwrap();
            log::trace!("peer_client_to_stream write_stream");
            match pack {
                TunnelArcPack::TunnelHello(value) => {
                    log::error!("err:TunnelHello => TunnelHello:{:?}", value);
                }
                TunnelArcPack::TunnelData(value) => {
                    log::trace!("peer_stream_write TunnelData:{:?}", value.header);
                    protopack::write_tunnel_data(&mut buf_writer, &value).await?;
                }
                TunnelArcPack::TunnelDataAck(value) => {
                    log::trace!("peer_stream_write TunnelDataAck:{:?}", value);
                    protopack::write_tunnel_data_ack(&mut buf_writer, &value).await?;
                }
                TunnelArcPack::TunnelClose(value) => {
                    log::trace!("peer_stream_write TunnelClose:{:?}", value);
                    protopack::write_pack(
                        &mut buf_writer,
                        TunnelHeaderType::TunnelClose,
                        value.as_ref(),
                        true,
                    )
                    .await?;
                }
                TunnelArcPack::TunnelHeartbeat(value) => {
                    log::trace!("peer_stream_write TunnelHeartbeat:{:?}", value);
                    protopack::write_pack(
                        &mut buf_writer,
                        TunnelHeaderType::TunnelHeartbeat,
                        value.as_ref(),
                        true,
                    )
                    .await?;
                }
                TunnelArcPack::TunnelHeartbeatAck(value) => {
                    log::trace!("peer_stream_write TunnelHeartbeatAck:{:?}", value);
                    protopack::write_pack(
                        &mut buf_writer,
                        TunnelHeaderType::TunnelHeartbeatAck,
                        value.as_ref(),
                        true,
                    )
                    .await?;
                }
                TunnelArcPack::TunnelCreateConnect(value) => {
                    log::trace!("peer_stream_write TunnelCreateConnect:{:?}", value);
                    protopack::write_pack(
                        &mut buf_writer,
                        TunnelHeaderType::TunnelCreateConnect,
                        value.as_ref(),
                        true,
                    )
                    .await?;
                }
            };
        }
    }

    async fn stream_flow(
        stream_info: Arc<std::sync::Mutex<StreamFlowInfo>>,
        peer_client_cmd_tx: mpsc::UnboundedSender<PeerClientCmd>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            let flow = {
                let mut stream_info = stream_info.lock().unwrap();
                let read = stream_info.read;
                let write = stream_info.write;
                stream_info.read = 0;
                stream_info.write = 0;
                (read, write)
            };
            peer_client_cmd_tx
                .send(PeerClientCmd::PeerStreamFlow(flow))
                .map_err(|_| anyhow::anyhow!("err:PeerStreamFlow"))?;
        }
    }
}
