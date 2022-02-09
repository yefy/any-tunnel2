use super::anychannel::AnyAsyncReceiver;
use super::anychannel::AnyUnboundedReceiver;
use super::peer_client::PeerClientCmd;
use super::protopack::TunnelArcPack;
use super::protopack::TunnelClose;
use super::protopack::TunnelData;
use super::protopack::TunnelDataAck;
use super::protopack::TunnelDataAckData;
use super::protopack::TunnelDataAckHeader;
use super::protopack::TunnelDataHeader;
use crate::protopack::TunnelHeartbeat;
use chrono::prelude::*;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

lazy_static! {
    pub static ref START_PACK_LEN: usize = 100;
    pub static ref LIMIT_MAX_SEND_PACK_LEN: usize = 3000;
}

#[derive(Clone)]
struct SendWait {
    max_send_pack_len: Arc<AtomicUsize>,
    curr_send_pack_len: Arc<AtomicUsize>,
    waker: Arc<std::sync::Mutex<Option<Waker>>>,
}

impl SendWait {
    pub fn new(
        max_send_pack_len: Arc<AtomicUsize>,
        curr_send_pack_len: Arc<AtomicUsize>,
    ) -> SendWait {
        SendWait {
            max_send_pack_len,
            curr_send_pack_len,
            waker: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub fn waker(&self) {
        self.curr_send_pack_len.fetch_sub(1, Ordering::Relaxed);
        let waker = self.waker.lock().unwrap().take();
        if waker.is_some() {
            waker.unwrap().wake();
        }
    }
}

impl Future for SendWait {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let curr_send_pack_len = self.curr_send_pack_len.load(Ordering::Relaxed);
        let max_send_pack_len = self.max_send_pack_len.load(Ordering::Relaxed);
        if curr_send_pack_len >= max_send_pack_len {
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

pub struct WaitingPackInfo {
    pub insert_timestamp: i64,
    pub first_send_timestamp: i64,
    pub pack_id: u32,
}

pub struct StreamPack {
    stream_id: u32,
    stream_pack_to_peer_stream_tx: async_channel::Sender<TunnelArcPack>,
    waiting_pack_map: Mutex<HashMap<u32, (TunnelArcPack, i64)>>,
    waiting_pack_infos: Mutex<LinkedList<WaitingPackInfo>>,
    waiting_pack_timeout: AtomicI64,
    heartbeat_count: AtomicU32,
    max_send_pack_len: Arc<AtomicUsize>,
    curr_send_pack_len: Arc<AtomicUsize>,
    tunnel_data_ack: Arc<Mutex<Option<TunnelDataAck>>>,
    last_recv_ack_time: Arc<AtomicI64>,
    curr_send_flow: AtomicUsize,
    max_send_flow: AtomicUsize,
    ack_delay_time: AtomicI64,
    send_wait: SendWait,
    flow_fall_count: AtomicUsize,
    first_ack_time: AtomicI64,
}

impl StreamPack {
    pub fn new(
        stream_id: u32,
        stream_pack_to_peer_stream_tx: async_channel::Sender<TunnelArcPack>,
    ) -> StreamPack {
        let max_send_pack_len = Arc::new(AtomicUsize::new(*START_PACK_LEN));
        let curr_send_pack_len = Arc::new(AtomicUsize::new(0));
        StreamPack {
            stream_id,
            stream_pack_to_peer_stream_tx,
            waiting_pack_map: Mutex::new(HashMap::new()),
            waiting_pack_infos: Mutex::new(LinkedList::new()),
            heartbeat_count: AtomicU32::new(0),
            waiting_pack_timeout: AtomicI64::new(1000 * 60),
            max_send_pack_len: max_send_pack_len.clone(),
            curr_send_pack_len: curr_send_pack_len.clone(),
            tunnel_data_ack: Arc::new(Mutex::new(None)),
            last_recv_ack_time: Arc::new(AtomicI64::new(Local::now().timestamp_millis())),
            curr_send_flow: AtomicUsize::new(0),
            max_send_flow: AtomicUsize::new(1),
            ack_delay_time: AtomicI64::new(50),
            send_wait: SendWait::new(max_send_pack_len, curr_send_pack_len),
            flow_fall_count: AtomicUsize::new(0),
            first_ack_time: AtomicI64::new(0),
        }
    }
    pub async fn start(
        &self,
        stream_pack_to_stream_tx: mpsc::Sender<Arc<TunnelData>>,
        peer_client_to_stream_pack_rx: AnyUnboundedReceiver<TunnelArcPack>,
        mut stream_to_stream_pack_rx: async_channel::Receiver<Vec<u8>>,
        peer_client_cmd_tx: mpsc::UnboundedSender<PeerClientCmd>,
    ) -> anyhow::Result<()> {
        let is_close = Arc::new(AtomicBool::new(false));
        let ret: anyhow::Result<()> = async {
            tokio::select! {
                biased;
                ret = self.peer_client_to_stream_pack(is_close.clone(), stream_pack_to_stream_tx, peer_client_to_stream_pack_rx) => {
                    ret.map_err(|e| anyhow::anyhow!("err:peer_client_to_stream_pack => e:{}", e))?;
                    Ok(())
                }
                ret = self.stream_to_stream_pack(is_close.clone(), &mut stream_to_stream_pack_rx) => {
                    ret.map_err(|e| anyhow::anyhow!("err:stream_to_stream_pack => e:{}", e))?;
                    Ok(())
                }
                ret = self.check_waiting_pack_timeout(peer_client_cmd_tx.clone()) => {
                    ret.map_err(|e| anyhow::anyhow!("err:check_waiting_pack_timeout => e:{}", e))?;
                    Ok(())
                }
                ret = self.heartbeat() => {
                    ret.map_err(|e| anyhow::anyhow!("err:heartbeat => e:{}", e))?;
                    Ok(())
                }
                ret = self.send_tunnel_data_ack() => {
                    ret.map_err(|e| anyhow::anyhow!("err:send_tunnel_data_ack => e:{}", e))?;
                    Ok(())
                }
                ret = self.check_send_flow() => {
                    ret.map_err(|e| anyhow::anyhow!("err:check_send_flow => e:{}", e))?;
                    Ok(())
                }
                else => {
                    return Err(anyhow::anyhow!("err:select"));
                }
            }
        }
            .await;
        let _ = peer_client_cmd_tx
            .send(PeerClientCmd::PackWaiting((self.stream_id, 0)))
            .map_err(|_| anyhow::anyhow!("err:PackWaiting"));
        log::debug!("send_pack_close");
        if let Err(_) = self.send_pack_close().await {
            log::debug!("send_pack_close err");
        }
        ret?;
        Ok(())
    }

    async fn peer_client_to_stream_pack(
        &self,
        is_close: Arc<AtomicBool>,
        stream_pack_to_stream_tx: mpsc::Sender<Arc<TunnelData>>,
        mut peer_client_to_stream_pack_rx: AnyUnboundedReceiver<TunnelArcPack>,
    ) -> anyhow::Result<()> {
        let mut pack_id = 1u32;
        let mut send_pack_id_map = HashMap::<u32, ()>::new();
        let mut recv_pack_cache_map = HashMap::<u32, Arc<TunnelData>>::new();
        loop {
            if is_close.load(Ordering::Relaxed) {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            let ret: anyhow::Result<()> = async {
                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(100),
                    peer_client_to_stream_pack_rx.recv(),
                )
                .await
                {
                    Ok(pack) => {
                        //let pack = peer_client_to_stream_pack_rx.recv().await;
                        if pack.is_none() {
                            log::debug!("peer_client_to_stream_pack close");
                            return Err(anyhow::anyhow!("err:peer_client_to_stream_pack_rx close"));
                        }
                        let pack = pack.unwrap();
                        match pack {
                            TunnelArcPack::TunnelHello(value) => {
                                log::error!("err:TunnelHello => TunnelHello:{:?}", value);
                            }
                            TunnelArcPack::TunnelCreateConnect(value) => {
                                log::error!(
                                    "err:TunnelCreateConnect => TunnelCreateConnect:{:?}",
                                    value
                                );
                            }
                            TunnelArcPack::TunnelData(value) => {
                                if value.header.stream_id != self.stream_id {
                                    log::error!("err:value.header.stream_id != self.stream_id");
                                    return Ok(());
                                }

                                log::trace!("TunnelData:{:?}", value.header);
                                if send_pack_id_map.get(&value.header.pack_id).is_none() {
                                    recv_pack_cache_map.insert(value.header.pack_id, value);

                                    while let Some(value) = recv_pack_cache_map.remove(&pack_id) {
                                        log::trace!("stream_pack_to_stream_tx:{:?}", value.header);
                                        if let Err(e) =
                                            stream_pack_to_stream_tx.try_send(value.clone())
                                        {
                                            match e {
                                                tokio::sync::mpsc::error::TrySendError::Full(
                                                    value,
                                                ) => {
                                                    log::trace!("stream_pack_to_stream_tx full");
                                                    recv_pack_cache_map.insert(pack_id, value);
                                                    break;
                                                }
                                                tokio::sync::mpsc::error::TrySendError::Closed(
                                                    _,
                                                ) => {
                                                    return Err(anyhow::anyhow!(
                                                        "err:stream_pack_to_stream_tx close"
                                                    ));
                                                }
                                            }
                                        } else {
                                            send_pack_id_map.insert(pack_id, ());
                                            pack_id += 1;
                                            #[cfg(feature = "tunnel-wait-ack")]
                                            {
                                                self.add_tunnel_data_ack(&value).await?;
                                            }
                                        }
                                    }
                                } else {
                                    #[cfg(feature = "tunnel-wait-ack")]
                                    {
                                        self.add_tunnel_data_ack(&value).await?;
                                    }
                                }
                            }
                            TunnelArcPack::TunnelDataAck(value) => {
                                if value.header.stream_id != self.stream_id {
                                    log::error!("err:stream_id != self.stream_id");
                                    return Ok(());
                                }
                                let mut first_send_time = 0;
                                {
                                    let mut waiting_pack_map = self.waiting_pack_map.lock().await;
                                    for pack_id in value.data.pack_id.iter() {
                                        log::trace!("pack_id drop:{:?}", pack_id);
                                        let pack = waiting_pack_map.remove(&pack_id);
                                        if pack.is_some() {
                                            let (pack, _first_send_time) = pack.unwrap();
                                            if first_send_time == 0 {
                                                first_send_time = _first_send_time;
                                            }
                                            match pack {
                                                TunnelArcPack::TunnelData(data) => {
                                                    self.curr_send_flow.fetch_add(
                                                        data.datas.len(),
                                                        Ordering::Relaxed,
                                                    );
                                                }
                                                _ => {}
                                            }
                                            self.send_wait.waker();
                                        }
                                    }
                                }

                                if first_send_time > 0 {
                                    let first_send_time = first_send_time
                                        - self.ack_delay_time.load(Ordering::Relaxed);
                                    if first_send_time > 0 {
                                        self.recv_ack_init(first_send_time).await;
                                    } else {
                                        log::debug!("first_send_time <= 0");
                                    }
                                }
                            }
                            TunnelArcPack::TunnelClose(value) => {
                                if value.stream_id != self.stream_id {
                                    log::error!("err:TunnelClose.stream_id != self.stream_id");
                                    return Ok(());
                                }
                                return Err(anyhow::anyhow!("err:TunnelClose"))?;
                            }
                            TunnelArcPack::TunnelHeartbeat(value) => {
                                self.send_heartbeat_ack(value).await?;
                            }
                            TunnelArcPack::TunnelHeartbeatAck(value) => {
                                self.recv_ack_init(value.time).await;
                            }
                        }
                    }
                    Err(_) => {
                        while let Some(value) = recv_pack_cache_map.remove(&pack_id) {
                            log::trace!("stream_pack_to_stream_tx:{:?}", value.header);
                            if let Err(e) = stream_pack_to_stream_tx.try_send(value.clone()) {
                                match e {
                                    tokio::sync::mpsc::error::TrySendError::Full(value) => {
                                        log::trace!("stream_pack_to_stream_tx full");
                                        recv_pack_cache_map.insert(pack_id, value);
                                        break;
                                    }
                                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                        return Err(anyhow::anyhow!(
                                            "err:stream_pack_to_stream_tx close"
                                        ));
                                    }
                                }
                            } else {
                                send_pack_id_map.insert(pack_id, ());
                                pack_id += 1;
                                #[cfg(feature = "tunnel-wait-ack")]
                                {
                                    self.add_tunnel_data_ack(&value).await?;
                                }
                            }
                        }
                    }
                }
                Ok(())
            }
            .await;
            if ret.is_err() {
                log::debug!("peer_client_to_stream_pack close");
                is_close.swap(true, Ordering::Relaxed);
            }
        }
    }

    async fn recv_ack_init(&self, first_send_time: i64) {
        let curr_time = Local::now().timestamp_millis();
        self.last_recv_ack_time.swap(curr_time, Ordering::Relaxed);
        let diff_time = curr_time - first_send_time;
        let first_ack_time = self.first_ack_time.load(Ordering::Relaxed);
        if first_ack_time == 0 {
            self.first_ack_time.swap(diff_time, Ordering::Relaxed);
        }
        let mut waiting_pack_timeout = diff_time * 3;
        if waiting_pack_timeout > 0 {
            if waiting_pack_timeout < 1000 * 3 {
                waiting_pack_timeout = 1000 * 3;
            }
            self.waiting_pack_timeout
                .swap(waiting_pack_timeout, Ordering::Relaxed);
        }
        self.heartbeat_count.swap(0, Ordering::Relaxed);
    }

    async fn stream_to_stream_pack(
        &self,
        is_close: Arc<AtomicBool>,
        stream_to_stream_pack_rx: &mut async_channel::Receiver<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let mut pack_id = 0;
        let mut curr_send_pack_len_timeout = 0;
        loop {
            let ret: anyhow::Result<()> = async {
                //curl http://www.upstream.cn:18080/5g.b --output a --limit-rate 100k
                //这里超时了也要尝试接受数据包，不然上游可能超时断开
                #[cfg(feature = "tunnel-wait-ack")]
                {
                    match tokio::time::timeout(
                        tokio::time::Duration::from_secs(1),
                        self.send_wait.clone(),
                    )
                    .await
                    {
                        Ok(_) => {
                            curr_send_pack_len_timeout = 0;
                        }
                        Err(_) => {
                            let curr_send_pack_len =
                                self.curr_send_pack_len.load(Ordering::Relaxed);
                            if curr_send_pack_len_timeout == 0 {
                                curr_send_pack_len_timeout = curr_send_pack_len;
                            }

                            if curr_send_pack_len > curr_send_pack_len_timeout {
                                return Ok(());
                            } else {
                                curr_send_pack_len_timeout = curr_send_pack_len;
                            }
                        }
                    }
                }

                let datas = match tokio::time::timeout(
                    tokio::time::Duration::from_secs(1),
                    stream_to_stream_pack_rx.recv(),
                )
                .await
                {
                    Ok(datas) => datas.map_err(|e| {
                        anyhow::anyhow!("err:stream_to_stream_pack_rx recv => e:{}", e)
                    })?,
                    Err(_) => {
                        return Ok(());
                    }
                };

                self.curr_send_pack_len.fetch_add(1, Ordering::Relaxed);
                pack_id += 1;
                let header = TunnelDataHeader {
                    stream_id: self.stream_id,
                    pack_id,
                    pack_size: datas.len() as u32,
                };
                log::trace!("stream_pack_to_peer_client:{:?}", header);
                let value = Arc::new(TunnelData { header, datas });
                self.stream_pack_to_peer_client(pack_id, TunnelArcPack::TunnelData(value), 0)
                    .await
                    .map_err(|e| anyhow::anyhow!("err:stream_pack_to_peer_client => e:{}", e))?;
                Ok(())
            }
            .await;
            if let Err(e) = ret {
                log::debug!("stream_to_stream_pack_rx close, e:{}", e);
                return Ok(());
            }

            if is_close.load(Ordering::Relaxed) {
                log::debug!("is_close stream_to_stream_pack_rx close");
                return Ok(());
            }
        }
    }

    async fn check_waiting_pack_timeout(
        &self,
        peer_client_cmd_tx: mpsc::UnboundedSender<PeerClientCmd>,
    ) -> anyhow::Result<()> {
        let diff_time = 2;
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(diff_time)).await;

            let waiting_pack_len = { self.waiting_pack_map.lock().await.len() as u32 };
            peer_client_cmd_tx
                .send(PeerClientCmd::PackWaiting((
                    self.stream_id,
                    waiting_pack_len,
                )))
                .map_err(|_| anyhow::anyhow!("err:PackWaiting"))?;

            let curr_timestamp = Local::now().timestamp_millis();
            loop {
                let waiting_pack_info = {
                    let mut waiting_pack_infos = self.waiting_pack_infos.lock().await;
                    let waiting_pack_info = waiting_pack_infos.front();
                    if waiting_pack_info.is_none() {
                        break;
                    }
                    let waiting_pack_info = waiting_pack_info.unwrap();
                    if curr_timestamp - waiting_pack_info.insert_timestamp
                        > self.waiting_pack_timeout.load(Ordering::Relaxed)
                    {
                        waiting_pack_infos.pop_front()
                    } else {
                        break;
                    }
                };

                if waiting_pack_info.is_none() {
                    break;
                }
                let waiting_pack_info = waiting_pack_info.unwrap();
                let waiting_pack = {
                    self.waiting_pack_map
                        .lock()
                        .await
                        .remove(&waiting_pack_info.pack_id)
                };
                if waiting_pack.is_none() {
                    continue;
                }
                let (waiting_pack, first_send_time) = waiting_pack.unwrap();
                self.stream_pack_to_peer_client(
                    waiting_pack_info.pack_id,
                    waiting_pack,
                    first_send_time,
                )
                .await?;
            }
        }
    }

    async fn heartbeat(&self) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            let max_send_pack_len = self.max_send_pack_len.load(Ordering::Relaxed);
            let curr_send_pack_len = self.curr_send_pack_len.load(Ordering::Relaxed);
            log::debug!(
                "max_send_pack_len:{}, curr_send_pack_len:{}",
                max_send_pack_len,
                curr_send_pack_len
            );
            log::debug!(
                "waiting_pack_timeout:{}",
                self.waiting_pack_timeout.load(Ordering::Relaxed)
            );
            log::debug!(
                "waiting_pack_len:{}",
                self.waiting_pack_map.lock().await.len()
            );
            let curr_time = Local::now().timestamp_millis();
            if curr_time - self.last_recv_ack_time.load(Ordering::Relaxed) < 1000 * 5 {
                continue;
            }
            self.send_heartbeat().await?;
            let heartbeat_count = self.heartbeat_count.fetch_add(1, Ordering::Relaxed);
            if heartbeat_count >= 20 {
                log::error!("heartbeat timeout");
                return Ok(());
            }
        }
    }

    async fn send_pack_close(&self) -> anyhow::Result<()> {
        let value = Arc::new(TunnelClose {
            stream_id: self.stream_id.clone(),
        });
        self.stream_pack_to_peer_stream_tx
            .send(TunnelArcPack::TunnelClose(value))
            .await?;
        Ok(())
    }

    async fn add_tunnel_data_ack(&self, value: &TunnelData) -> anyhow::Result<()> {
        log::trace!("TunnelDataAck:{:?}", value.header);
        let mut tunnel_data_ack = self.tunnel_data_ack.lock().await;
        if tunnel_data_ack.is_none() {
            let mut ack = TunnelDataAck {
                header: TunnelDataAckHeader {
                    stream_id: value.header.stream_id,
                    data_size: 0,
                },
                data: TunnelDataAckData {
                    pack_id: Vec::with_capacity(100),
                },
            };
            ack.data.pack_id.push(value.header.pack_id);
            *tunnel_data_ack = Some(ack);
        } else {
            tunnel_data_ack
                .as_mut()
                .unwrap()
                .data
                .pack_id
                .push(value.header.pack_id);
        }
        Ok(())
    }

    async fn check_send_flow(&self) -> anyhow::Result<()> {
        loop {
            let first_ack_time = self.first_ack_time.load(Ordering::Relaxed);
            if first_ack_time == 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                continue;
            }
            let mut first_ack_time = first_ack_time * 2;
            if first_ack_time > 500 {
                first_ack_time = 500;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(first_ack_time as u64)).await;
            let curr_send_pack_len = self.curr_send_pack_len.load(Ordering::Relaxed);
            let max_send_pack_len = self.max_send_pack_len.load(Ordering::Relaxed);
            if curr_send_pack_len > max_send_pack_len {
                continue;
            }

            let max_send_flow = self.max_send_flow.load(Ordering::Relaxed);
            let curr_send_flow = self.curr_send_flow.swap(0, Ordering::Relaxed);
            if curr_send_flow > max_send_flow {
                self.max_send_flow.swap(curr_send_flow, Ordering::Relaxed);
                let max_send_pack_len = if max_send_pack_len < 800 {
                    max_send_pack_len * 2
                } else {
                    if max_send_pack_len < *LIMIT_MAX_SEND_PACK_LEN {
                        max_send_pack_len + 200
                    } else {
                        max_send_pack_len
                    }
                };

                log::debug!("max_send_pack_len:{}", max_send_pack_len);
                self.max_send_pack_len
                    .swap(max_send_pack_len, Ordering::Relaxed);
            }

            if curr_send_flow < max_send_flow / 3 && curr_send_pack_len == max_send_pack_len {
                let flow_fall_count = self.flow_fall_count.fetch_add(1, Ordering::Relaxed) + 1;
                if flow_fall_count > 20 {
                    log::debug!("reset max_send_pack_len:{}", *START_PACK_LEN);
                    self.first_ack_time.swap(0, Ordering::Relaxed);
                    self.flow_fall_count.swap(0, Ordering::Relaxed);
                    self.max_send_flow.swap(1, Ordering::Relaxed);
                    self.max_send_pack_len
                        .swap(*START_PACK_LEN, Ordering::Relaxed);
                }
            } else {
                self.flow_fall_count.swap(0, Ordering::Relaxed);
            }
        }
    }

    async fn send_tunnel_data_ack(&self) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                self.ack_delay_time.load(Ordering::Relaxed) as u64,
            ))
            .await;
            let tunnel_data_ack = {
                let tunnel_data_ack = self.tunnel_data_ack.lock().await.take();
                if tunnel_data_ack.is_none() {
                    continue;
                }
                tunnel_data_ack.unwrap()
            };
            //log::debug!("tunnel_data_ack pack_id len:{:?}", tunnel_data_ack.data.pack_id.len());
            let _ = self
                .stream_pack_to_peer_stream_tx
                .send(TunnelArcPack::TunnelDataAck(Arc::new(tunnel_data_ack)))
                .await;
        }
    }

    async fn send_heartbeat(&self) -> anyhow::Result<()> {
        let value = Arc::new(TunnelHeartbeat {
            stream_id: self.stream_id,
            time: Local::now().timestamp_millis(),
        });

        self.stream_pack_to_peer_stream_tx
            .send(TunnelArcPack::TunnelHeartbeat(value))
            .await?;

        Ok(())
    }

    async fn send_heartbeat_ack(&self, value: Arc<TunnelHeartbeat>) -> anyhow::Result<()> {
        let value = Arc::new(TunnelHeartbeat {
            stream_id: value.stream_id,
            time: value.time,
        });

        self.stream_pack_to_peer_stream_tx
            .send(TunnelArcPack::TunnelHeartbeatAck(value))
            .await?;

        Ok(())
    }

    async fn stream_pack_to_peer_client(
        &self,
        pack_id: u32,
        pack: TunnelArcPack,
        first_send_time: i64,
    ) -> anyhow::Result<()> {
        #[cfg(feature = "tunnel-wait-ack")]
        {
            let curr_timestamp = Local::now().timestamp_millis();
            let first_send_time = if first_send_time > 0 {
                first_send_time
            } else {
                curr_timestamp
            };
            {
                {
                    self.waiting_pack_infos
                        .lock()
                        .await
                        .push_back(WaitingPackInfo {
                            insert_timestamp: curr_timestamp,
                            first_send_timestamp: first_send_time,
                            pack_id: pack_id,
                        });
                }

                {
                    self.waiting_pack_map
                        .lock()
                        .await
                        .insert(pack_id, (pack.clone(), first_send_time));
                }
            }
        }
        self.stream_pack_to_peer_stream_tx.send(pack).await?;
        Ok(())
    }
}
