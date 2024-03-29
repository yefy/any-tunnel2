use super::protopack::TunnelData;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

struct StreamBuf {
    tunnel_data: Arc<TunnelData>,
    pos: usize,
}

impl StreamBuf {
    fn new(tunnel_data: Arc<TunnelData>) -> StreamBuf {
        StreamBuf {
            tunnel_data,
            pos: 0,
        }
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.tunnel_data.datas.len() - self.pos
    }

    #[inline]
    fn split_to(&mut self, at: usize) -> &[u8] {
        let mut end = self.pos + at;
        if end > self.tunnel_data.datas.len() {
            end = self.tunnel_data.datas.len();
        }
        let pos = self.pos;
        self.pos = end;
        &self.tunnel_data.datas.as_slice()[pos..end]
    }
}

pub struct Stream {
    stream_tx: Option<async_channel::Sender<Vec<u8>>>,
    stream_rx: Option<mpsc::Receiver<Arc<TunnelData>>>,
    buf: Option<StreamBuf>,
    send: Option<
        Pin<
            Box<
                dyn Future<Output = std::result::Result<(), async_channel::SendError<Vec<u8>>>>
                    + std::marker::Send,
            >,
        >,
    >,
    send_len: usize,
}

impl Stream {
    pub fn new(
        stream_tx: async_channel::Sender<Vec<u8>>,
        stream_rx: mpsc::Receiver<Arc<TunnelData>>,
    ) -> Stream {
        Stream {
            stream_tx: Some(stream_tx),
            stream_rx: Some(stream_rx),
            buf: None,
            send: None,
            send_len: 0,
        }
    }

    pub fn close(&mut self) {
        let stream_tx = self.stream_tx.take();
        let stream_rx = self.stream_rx.take();
        if stream_tx.is_some() {
            log::debug!("close stream_tx");
            let stream_tx = stream_tx.unwrap();
            stream_tx.close();
            std::mem::drop(stream_tx);
        }
        if stream_rx.is_some() {
            log::debug!("close stream_rx");
            let mut stream_rx = stream_rx.unwrap();
            stream_rx.close();
            std::mem::drop(stream_rx);
        }
    }

    async fn read_channel(&mut self, buf: &mut tokio::io::ReadBuf<'_>) -> io::Result<()> {
        if self.stream_rx.is_none() {
            return Ok(());
        }
        loop {
            if self.buf.is_some() {
                let cache_buf = self.buf.as_mut().unwrap();
                let remain = cache_buf.remaining();
                if remain > 0 {
                    let expected = buf.initialize_unfilled().len();
                    let split_at = std::cmp::min(expected, remain);
                    let data = cache_buf.split_to(split_at);
                    buf.put_slice(data);
                    return Ok(());
                } else {
                    self.buf = None;
                }
            }

            let slice = self.stream_rx.as_mut().unwrap().recv().await;
            if slice.is_none() {
                self.close();
                return Ok(());
            }
            let slice = slice.unwrap();
            self.buf = Some(StreamBuf::new(slice));
        }
    }
}

impl tokio::io::AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut read_fut = Box::pin(self.read_channel(buf));
        let ret = read_fut.as_mut().poll(cx);
        match ret {
            Poll::Ready(ret) => Poll::Ready(ret),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl tokio::io::AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.stream_tx.is_none() {
            return Poll::Ready(Ok(0));
        }

        let mut send = if self.send.is_some() {
            self.send.take().unwrap()
        } else {
            let len = buf.len();
            self.send_len = len;
            let mut slice = Vec::with_capacity(len);
            slice.extend_from_slice(buf);
            let stream_tx = self.stream_tx.clone().unwrap();
            Box::pin(async move { stream_tx.send(slice).await })
        };

        let send_len = self.send_len;
        let ret = send.as_mut().poll(cx);
        match ret {
            Poll::Ready(ret) => {
                if ret.is_err() {
                    self.close();
                    return Poll::Ready(Ok(0));
                }
                Poll::Ready(Ok(send_len))
            }
            Poll::Pending => {
                //Pending的时候保存起来
                self.send = Some(send);
                self.send_len = send_len;
                return Poll::Pending;
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
