use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use hashbrown::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

#[async_trait]
pub trait AnyAsyncSender {
    type AnyMsg;
    async fn send(&self, _: Self::AnyMsg) -> Result<()>;
}

#[async_trait]
pub trait AnyAsyncReceiver {
    type AnyMsg;
    async fn recv(&mut self) -> Option<Self::AnyMsg>;
}

pub struct AnyUnboundedSender<L> {
    pub tx: mpsc::UnboundedSender<L>,
    marker: PhantomData<L>,
}

#[async_trait]
impl<L: std::marker::Sync + std::marker::Send> AnyAsyncSender for AnyUnboundedSender<L> {
    type AnyMsg = L;
    async fn send(&self, any_msg: Self::AnyMsg) -> Result<()> {
        if let Err(_) = self.tx.send(any_msg) {
            return Err(anyhow!("er:AnyUnboundedSender send"));
        }
        Ok(())
    }
}

pub struct AnySender<L> {
    pub tx: mpsc::Sender<L>,
    marker: PhantomData<L>,
}

#[async_trait]
impl<L: std::marker::Sync + std::marker::Send> AnyAsyncSender for AnySender<L> {
    type AnyMsg = L;
    async fn send(&self, any_msg: Self::AnyMsg) -> Result<()> {
        if let Err(_) = self.tx.send(any_msg).await {
            return Err(anyhow!("er:AnySender send"));
        }
        Ok(())
    }
}

pub struct AnyUnboundedReceiver<L> {
    pub rx: mpsc::UnboundedReceiver<L>,
    marker: PhantomData<L>,
}

#[async_trait]
impl<L: std::marker::Sync + std::marker::Send> AnyAsyncReceiver for AnyUnboundedReceiver<L> {
    type AnyMsg = L;
    async fn recv(&mut self) -> Option<Self::AnyMsg> {
        self.rx.recv().await
    }
}

pub struct AnyReceiver<L> {
    pub rx: mpsc::Receiver<L>,
    marker: PhantomData<L>,
}

#[async_trait]
impl<L: std::marker::Sync + std::marker::Send> AnyAsyncReceiver for AnyReceiver<L> {
    type AnyMsg = L;
    async fn recv(&mut self) -> Option<Self::AnyMsg> {
        self.rx.recv().await
    }
}

#[async_trait]
pub trait AnyAsyncChannel {
    type Sender: AnyAsyncSender;
    type Receiver: AnyAsyncReceiver;
    fn channel(buffer: usize) -> (Self::Sender, Self::Receiver);
}

pub struct AnyUnboundedChannel<L> {
    marker: PhantomData<L>,
}

#[async_trait]
impl<L: std::marker::Sync + std::marker::Send> AnyAsyncChannel for AnyUnboundedChannel<L> {
    type Sender = AnyUnboundedSender<L>;
    type Receiver = AnyUnboundedReceiver<L>;
    fn channel(_buffer: usize) -> (Self::Sender, Self::Receiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            AnyUnboundedSender::<L> {
                tx,
                marker: PhantomData::default(),
            },
            AnyUnboundedReceiver::<L> {
                rx,
                marker: PhantomData::default(),
            },
        )
    }
}

pub struct AnyChannel<L> {
    marker: PhantomData<L>,
}

#[async_trait]
impl<L: std::marker::Sync + std::marker::Send> AnyAsyncChannel for AnyChannel<L> {
    type Sender = AnySender<L>;
    type Receiver = AnyReceiver<L>;
    fn channel(buffer: usize) -> (Self::Sender, Self::Receiver) {
        let (tx, rx) = mpsc::channel(buffer);
        (
            AnySender::<L> {
                tx,
                marker: PhantomData::default(),
            },
            AnyReceiver::<L> {
                rx,
                marker: PhantomData::default(),
            },
        )
    }
}

pub enum AnyAsyncSenderErr<AS: AnyAsyncChannel> {
    None(<<AS as AnyAsyncChannel>::Sender as AnyAsyncSender>::AnyMsg),
    Close(<<AS as AnyAsyncChannel>::Sender as AnyAsyncSender>::AnyMsg),
    Err(anyhow::Error),
    Ok,
}

pub struct AnyAsyncChannelMap<K: std::cmp::Eq + std::hash::Hash, AS: AnyAsyncChannel> {
    tx_map: HashMap<K, Option<AS::Sender>>,
}

impl<K: std::cmp::Eq + std::hash::Hash, AS: AnyAsyncChannel> AnyAsyncChannelMap<K, AS> {
    pub fn new() -> AnyAsyncChannelMap<K, AS> {
        AnyAsyncChannelMap {
            tx_map: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        let mut is_some = false;
        for (_, value) in self.tx_map.iter() {
            if value.is_some() {
                is_some = true;
                break;
            }
        }
        is_some
    }
    pub fn channel(&mut self, buffer: usize, key: K) -> AS::Receiver {
        let (tx, rx) = AS::channel(buffer);
        self.tx_map.insert(key, Some(tx));
        rx
    }

    pub fn remove(&mut self, key: K) {
        let mut value = self.tx_map.get_mut(&key);
        if value.is_some() {
            let value = value.as_mut().unwrap();
            if value.is_some() {
                std::mem::drop(value.take().unwrap());
            }
        }
    }

    pub fn remove_all(&mut self) -> bool {
        let mut is_drop = false;
        for (_, value) in self.tx_map.iter_mut() {
            if value.is_some() {
                std::mem::drop(value.take().unwrap());
                is_drop = true;
            }
        }
        is_drop
    }

    pub async fn send(
        &mut self,
        key: K,
        any_msg: <<AS as AnyAsyncChannel>::Sender as AnyAsyncSender>::AnyMsg,
    ) -> AnyAsyncSenderErr<AS> {
        match self.tx_map.get(&key) {
            Some(tx) => match tx {
                Some(tx) => {
                    if let Err(_) = tx.send(any_msg).await {
                        let value = self.tx_map.get_mut(&key).as_mut().unwrap().take().unwrap();
                        std::mem::drop(value);
                        AnyAsyncSenderErr::Err(anyhow!("err:AnyAsyncChannelMap send"))
                    } else {
                        AnyAsyncSenderErr::Ok
                    }
                }
                None => AnyAsyncSenderErr::Close(any_msg),
            },
            None => AnyAsyncSenderErr::None(any_msg),
        }
    }
}

pub struct AnyAsyncChannelRoundMap<AS: AnyAsyncChannel> {
    keys: Vec<usize>,
    key: AtomicUsize,
    index: AtomicUsize,
    tx_map: HashMap<usize, AS::Sender>,
}

impl<AS: AnyAsyncChannel> AnyAsyncChannelRoundMap<AS> {
    pub fn new() -> AnyAsyncChannelRoundMap<AS> {
        AnyAsyncChannelRoundMap {
            keys: Vec::new(),
            key: AtomicUsize::new(1),
            index: AtomicUsize::new(0),
            tx_map: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn key(&self) -> usize {
        self.key.fetch_add(1, Ordering::Relaxed)
    }

    pub fn index(&self) -> Option<usize> {
        let keys_len = self.keys.len();
        if keys_len <= 0 {
            return None;
        }
        let index = self.index.fetch_add(1, Ordering::Relaxed);
        let index = index % keys_len;
        Some(index)
    }

    pub fn remove(&mut self, key: usize) {
        for (index, key_) in self.keys.iter().enumerate() {
            if *key_ == key {
                self.keys.remove(index);
                break;
            }
        }
        let value = self.tx_map.remove(&key);
        if value.is_some() {
            std::mem::drop(value.unwrap());
        }
    }

    pub fn channel(&mut self, buffer: usize) -> (usize, AS::Receiver) {
        let (tx, rx) = AS::channel(buffer);
        let key = self.key();
        self.keys.push(key);
        self.tx_map.insert(key, tx);
        (key, rx)
    }

    pub async fn send(
        &mut self,
        any_msg: <<AS as AnyAsyncChannel>::Sender as AnyAsyncSender>::AnyMsg,
    ) -> Result<()> {
        let index = self.index();
        if index.is_none() {
            return Err(anyhow!("AnySenderMapRound index nil"))?;
        }
        let index = index.unwrap();
        let key = self.keys[index];
        match self.tx_map.get(&key) {
            Some(tx) => {
                if let Err(e) = tx.send(any_msg).await {
                    let value = self.tx_map.remove(&key);
                    if value.is_some() {
                        std::mem::drop(value.unwrap());
                    }
                    self.keys.remove(index);
                    return Err(e)?;
                }
                Ok(())
            }
            None => {
                self.keys.remove(index);
                return Err(anyhow!("AnySenderMapRound key nil"))?;
            }
        }
    }
}
