use anyhow::Result;
use bytes::BufMut;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::info;

use crate::{
    cmd::{Command, CommandExecutor},
    Backend, RespDecode, RespEncode, RespError, RespFrame,
};

#[derive(Debug)]
struct RespFrameCodec;

#[derive(Debug)]
struct RedisRequest {
    frame: RespFrame,
    backend: Backend,
}

#[derive(Debug)]
struct RedisResponse {
    frame: RespFrame,
}

pub async fn stream_handler(stream: TcpStream, backend: Backend) -> Result<()> {
    // how to get a frame from the stream?
    let mut framed = Framed::new(stream, RespFrameCodec);
    loop {
        let backend = backend.clone();
        match framed.next().await {
            Some(Ok(frame)) => {
                info!("Received frame: {:?}", frame);
                let request = RedisRequest { frame, backend };
                // call request_handler with the frame

                let response = request_handler(request).await?;
                info!("Sending response: {:?}", response.frame);
                // send the response back to the stream
                framed.send(response.frame).await?;
            }
            Some(Err(e)) => return Err(e), // 返回错误
            None => return Ok(()),         // 没有数据
        }
    }
}

async fn request_handler(request: RedisRequest) -> Result<RedisResponse> {
    let (frame, backend) = (request.frame, request.backend);
    let cmd = Command::try_from(frame)?;
    let frame = cmd.execute(&backend);
    Ok(RedisResponse { frame })
}

impl Encoder<RespFrame> for RespFrameCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: RespFrame,
        dst: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<(), Self::Error> {
        let encoded = item.encode();
        dst.put_slice(&encoded);
        Ok(())
    }
}

impl Decoder for RespFrameCodec {
    type Item = RespFrame;

    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<Option<Self::Item>, Self::Error> {
        match RespFrame::decode(src) {
            Ok(frame) => Ok(Some(frame)),
            Err(RespError::NotComplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
