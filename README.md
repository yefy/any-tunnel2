# any-tunnel2
any-tunnel2 建立在tcp，quic，srt等可靠协议之上

[dependencies]  
any-tunnel2 = { git = "https://github.com/yefy/any-tunnel2.git", branch = "main" }

# 原理
原来通过一条连接收发的数据包，现在借助共享N条长连接来进行收发，达到提速的效果。  
主要是为了解决高延迟带宽单连接带宽低问题，短连接加速频繁连接和断开握手问题  

#与any-tunnel区别
any-tunnel 1:N  
创建N个短连接，为一个连接加速  

any-tunnel2 M:N  
创建N个共享长连接建立一个隧道，可以在隧道上创建连接，创建连接非常快，不需要连接握手和断开握手，
可以解决短连接加速频繁连接和断开握手问题  

# example
cargo run --example server  
cargo run --example client  
