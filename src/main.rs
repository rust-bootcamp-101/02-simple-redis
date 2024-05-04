use anyhow::Result;
use simple_redis::{network, Backend};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    info!("Simple-Redis-Server is listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    let backend = Backend::new();
    loop {
        let cloned_backend = backend.clone();
        let (stream, remote_addr) = listener.accept().await?;
        info!("Accepted connection from: {}", addr);
        tokio::spawn(async move {
            if let Err(e) = network::stream_handler(stream, cloned_backend).await {
                warn!("handle error for {}: {:?}", remote_addr, e);
            }
        });
    }
}
