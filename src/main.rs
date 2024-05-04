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
    let backend = Backend::default();
    loop {
        let backend_cloned = backend.clone();
        let (stream, raddr) = listener.accept().await?;
        info!("Accepted connection from {}", raddr);
        tokio::spawn(async move {
            match network::stream_handler(stream, backend_cloned).await {
                Ok(_) => {
                    info!("Connection from {} closed", raddr);
                }
                Err(err) => {
                    warn!("handle error for {}: {:?}", raddr, err);
                }
            }
        });
    }
}
