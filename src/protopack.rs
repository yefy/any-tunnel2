use anyhow::anyhow;
use anyhow::Result;
use serde::ser;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const TUNNEL_MAX_HEADER_SIZE: usize = 4096;
pub const TUNNEL_VERSION: &'static str = "tunnel2.0.1.0";

//TunnelHeaderSize_u16 TunnelHeader TunnelHello
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelHeader {
    pub header_type: u8,
    pub body_size: u16,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TunnelHeaderType {
    TunnelMin = 10,
    TunnelHello = 11,
    TunnelData = 12,
    TunnelDataAck = 13,
    TunnelClose = 14,
    TunnelHeartbeat = 15,
    TunnelHeartbeatAck = 16,
    TunnelCreateConnect = 17,
    TunnelMax = 18,
}

#[derive(Clone, Debug)]
pub enum TunnelPack {
    TunnelHello(TunnelHello),
    TunnelData(TunnelData),
    TunnelDataAck(TunnelDataAck),
    TunnelClose(TunnelClose),
    TunnelHeartbeat(TunnelHeartbeat),
    TunnelHeartbeatAck(TunnelHeartbeat),
    TunnelCreateConnect(TunnelCreateConnect),
}

#[derive(Clone, Debug)]
pub enum TunnelArcPack {
    TunnelHello(Arc<TunnelHello>),
    TunnelData(Arc<TunnelData>),
    TunnelDataAck(Arc<TunnelDataAck>),
    TunnelClose(Arc<TunnelClose>),
    TunnelHeartbeat(Arc<TunnelHeartbeat>),
    TunnelHeartbeatAck(Arc<TunnelHeartbeat>),
    TunnelCreateConnect(Arc<TunnelCreateConnect>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelHello {
    pub version: String,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelDataHeader {
    pub stream_id: u32,
    pub pack_id: u32,
    pub pack_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TunnelData {
    pub header: TunnelDataHeader,
    pub datas: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelDataAckHeader {
    pub stream_id: u32,
    pub data_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelDataAckData {
    pub pack_id: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelDataAck {
    pub header: TunnelDataAckHeader,
    pub data: TunnelDataAckData,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelClose {
    pub stream_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelHeartbeat {
    pub stream_id: u32,
    pub time: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelCreateConnect {
    pub stream_id: u32,
}

pub async fn read_tunnel_hello<R: AsyncRead + std::marker::Unpin>(
    buf_reader: &mut R,
) -> Result<Option<TunnelHello>> {
    let mut slice = [0u8; TUNNEL_MAX_HEADER_SIZE];
    let pack = read_pack(buf_reader, &mut slice)
        .await
        .map_err(|e| anyhow!("err:read_pack => e:{}", e))?;
    match pack {
        TunnelPack::TunnelHello(tunnel_hello) => {
            return Ok(Some(tunnel_hello));
        }
        _ => return Err(anyhow!("err:not tunnel_hello")),
    }
}

pub async fn write_tunnel_data<R: AsyncWrite + std::marker::Unpin>(
    buf_writer: &mut R,
    value: &TunnelData,
) -> Result<()> {
    let typ = TunnelHeaderType::TunnelData;
    write_pack(buf_writer, typ, &value.header, false)
        .await
        .map_err(|e| anyhow!("err:write_pack => e:{}", e))?;
    buf_writer
        .write_all(&value.datas)
        .await
        .map_err(|e| anyhow!("err:buf_writer.write_all => e:{}", e))?;
    log::trace!("write_tunnel_data datas len:{:?}", value.datas.len());
    buf_writer
        .flush()
        .await
        .map_err(|e| anyhow!("err:buf_writer.flush => e:{}", e))?;
    Ok(())
}

pub async fn write_tunnel_data_ack<R: AsyncWrite + std::marker::Unpin>(
    buf_writer: &mut R,
    value: &TunnelDataAck,
) -> Result<()> {
    let data_slice =
        toml::to_vec(&value.data).map_err(|e| anyhow!("err:toml::to_vec => e:{}", e))?;
    let header = TunnelDataAckHeader {
        stream_id: value.header.stream_id,
        data_size: data_slice.len() as u32,
    };
    let typ = TunnelHeaderType::TunnelDataAck;
    write_pack(buf_writer, typ, &header, false)
        .await
        .map_err(|e| anyhow!("err:write_pack => e:{}", e))?;
    buf_writer
        .write_all(&data_slice)
        .await
        .map_err(|e| anyhow!("err:buf_writer.write_all => e:{}", e))?;
    log::trace!("write_tunnel_data_ack datas len:{:?}", data_slice.len());
    buf_writer
        .flush()
        .await
        .map_err(|e| anyhow!("err:buf_writer.flush => e:{}", e))?;
    Ok(())
}

pub async fn write_pack<T: ?Sized, W: AsyncWrite + std::marker::Unpin>(
    buf_writer: &mut W,
    typ: TunnelHeaderType,
    value: &T,
    is_flush: bool,
) -> Result<()>
where
    T: ser::Serialize,
{
    let slice = toml::to_vec(value).map_err(|e| anyhow!("err:toml::to_vec => e:{}", e))?;
    let header = TunnelHeader {
        header_type: typ as u8,
        body_size: slice.len() as u16,
    };
    let header_slice = toml::to_vec(&header).map_err(|e| anyhow!("err:toml::to_vec => e:{}", e))?;
    buf_writer
        .write_u16(header_slice.len() as u16)
        .await
        .map_err(|e| anyhow!("err:buf_writer.write_u16 => e:{}", e))?;
    log::trace!("write_pack header len:{:?}", header_slice.len());
    buf_writer
        .write_all(&header_slice)
        .await
        .map_err(|e| anyhow!("err:buf_writer.write_all => e:{}", e))?;
    log::trace!("write_pack header:{:?}", header);
    buf_writer
        .write_all(&slice)
        .await
        .map_err(|e| anyhow!("err:buf_writer.write_all => e:{}", e))?;
    log::trace!("write_pack body len:{:?}", slice.len());
    if is_flush {
        buf_writer
            .flush()
            .await
            .map_err(|e| anyhow!("err:buf_writer.flush => e:{}", e))?;
    }
    Ok(())
}

pub async fn read_pack<R: AsyncRead + std::marker::Unpin>(
    buf_reader: &mut R,
    slice: &mut [u8],
) -> Result<TunnelPack> {
    //let mut slice = [0u8; TUNNEL_MAX_HEADER_SIZE];
    let header_size = buf_reader
        .read_u16()
        .await
        .map_err(|e| anyhow!("err:header_size => e:{}", e))?;

    log::trace!("header_size:{}", header_size);
    if header_size as usize > slice.len() || header_size <= 0 {
        return Err(anyhow!(
            "err:header_size > slice.len() => header_size:{}",
            header_size
        ));
    }

    let header_slice = &mut slice[..header_size as usize];
    buf_reader
        .read_exact(header_slice)
        .await
        .map_err(|e| anyhow!("err:header_slice => e:{}", e))?;
    let header: TunnelHeader =
        toml::from_slice(header_slice).map_err(|e| anyhow!("err:TunnelData header=> e:{}", e))?;

    if header.header_type <= TunnelHeaderType::TunnelMin as u8
        || header.header_type >= TunnelHeaderType::TunnelMax as u8
    {
        return Err(anyhow!("err:TunnelData header_type"));
    }

    if header.body_size as usize > slice.len() {
        return Err(anyhow!("err:TunnelData body_size"));
    }
    log::trace!("read_pack header:{:?}", header);

    let body_slice = &mut slice[..header.body_size as usize];
    if header.body_size > 0 {
        buf_reader
            .read_exact(body_slice)
            .await
            .map_err(|e| anyhow!("err:body_slice => e:{}", e))?;
    }

    if header.header_type == TunnelHeaderType::TunnelHello as u8 {
        let value: TunnelHello =
            toml::from_slice(body_slice).map_err(|e| anyhow!("err:TunnelDataAck=> e:{}", e))?;
        Ok(TunnelPack::TunnelHello(value))
    } else if header.header_type == TunnelHeaderType::TunnelData as u8 {
        let header: TunnelDataHeader =
            toml::from_slice(body_slice).map_err(|e| anyhow!("err:TunnelData=> e:{}", e))?;
        log::trace!("read_pack body:{:?}", header);
        let mut datas = vec![0u8; header.pack_size as usize];
        let mut datas_slice = datas.as_mut_slice();
        buf_reader
            .read_exact(&mut datas_slice)
            .await
            .map_err(|e| anyhow!("err:datas => e:{}", e))?;

        log::trace!("read_pack datas.len:{:?}", datas.len());
        Ok(TunnelPack::TunnelData(TunnelData { header, datas }))
    } else if header.header_type == TunnelHeaderType::TunnelDataAck as u8 {
        let header: TunnelDataAckHeader = toml::from_slice(body_slice)
            .map_err(|e| anyhow!("err:TunnelDataAckHeader=> e:{}", e))?;
        log::trace!("read_pack body:{:?}", header);
        let mut datas = vec![0u8; header.data_size as usize];
        let mut datas_slice = datas.as_mut_slice();
        buf_reader
            .read_exact(&mut datas_slice)
            .await
            .map_err(|e| anyhow!("err:datas => e:{}", e))?;
        log::trace!("read_pack datas.len:{:?}", datas_slice.len());
        let data: TunnelDataAckData = toml::from_slice(datas_slice)?;
        Ok(TunnelPack::TunnelDataAck(TunnelDataAck { header, data }))
    } else if header.header_type == TunnelHeaderType::TunnelClose as u8 {
        let value: TunnelClose =
            toml::from_slice(body_slice).map_err(|e| anyhow!("err:TunnelClose=> e:{}", e))?;
        Ok(TunnelPack::TunnelClose(value))
    } else if header.header_type == TunnelHeaderType::TunnelHeartbeat as u8 {
        let value: TunnelHeartbeat =
            toml::from_slice(body_slice).map_err(|e| anyhow!("err:TunnelClose=> e:{}", e))?;
        Ok(TunnelPack::TunnelHeartbeat(value))
    } else if header.header_type == TunnelHeaderType::TunnelHeartbeatAck as u8 {
        let value: TunnelHeartbeat =
            toml::from_slice(body_slice).map_err(|e| anyhow!("err:TunnelClose=> e:{}", e))?;
        Ok(TunnelPack::TunnelHeartbeatAck(value))
    } else if header.header_type == TunnelHeaderType::TunnelCreateConnect as u8 {
        let value: TunnelCreateConnect =
            toml::from_slice(body_slice).map_err(|e| anyhow!("err:TunnelClose=> e:{}", e))?;
        Ok(TunnelPack::TunnelCreateConnect(value))
    } else {
        Err(anyhow!("err:TunnelData type"))
    }
}
