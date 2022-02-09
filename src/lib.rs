pub mod anychannel;
pub mod client;
pub mod peer_client;
pub mod peer_stream;
pub mod peer_stream_connect;
pub mod protopack;
pub mod rt;
pub mod server;
pub mod stream;
pub mod stream_flow;
pub mod stream_pack;
pub mod tunnel;

#[derive(Clone, Eq, PartialEq)]
pub enum Protocol4 {
    TCP,
    UDP,
}

impl Protocol4 {
    pub fn to_string(&self) -> String {
        match self {
            Protocol4::TCP => "tcp".to_string(),
            Protocol4::UDP => "udp".to_string(),
        }
    }
}
