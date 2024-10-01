use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::io::{self, Cursor};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

use mini_redis::frame::Error::Incomplete;
use mini_redis::{Frame, Result};
use tokio::net::{TcpListener, TcpStream};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;
struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }
    /// コネクションからフレームを読み取る
    ///
    /// EOF に到達したら `None` を返す

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // バッファされたデータからフレームをパースすることを試みる
            // 十分な量のデータがバッファに蓄えられていたら、ここでフレームを return する
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // バッファデータが足りなかった場合
            // ソケットからさらにデータを読み取ることを試みる
            //
            // 成功した場合、バイト数が返ってくる
            // `0` は "ストリームの終わり" を意味する
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // 相手側がコネクションを閉じた。
                // きれいにシャットダウンするため、read バッファのデータが空になる
                // ようにしなければならない。もしデータが残っているなら、それは
                // 相手がフレームを送信している途中でソケットを閉じたということを意味する
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        // `Buf` 型を作る
        let mut buf = Cursor::new(&self.buffer[..]);
        // フレーム全体が取得可能かどうかチェックする
        match Frame::check(&mut buf) {
            Ok(_) => {
                // フレームのバイト長を取得
                let len = buf.position() as usize;

                // parse を呼ぶために内部カーソルをリセットする
                buf.set_position(0);

                // フレームをパースする
                let frame = Frame::parse(&mut buf)?;

                // バッファからフレーム文を読み捨てる
                self.buffer.advance(len);

                Ok(Some(frame))
            }

            // 十分な量のバッファーが確保されていなかった場合
            Err(Incomplete) => Ok(None),
            // エラーが発生した場合
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            },
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            },
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
                
            },
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();
    
                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unimplemented!(),
        }

        let _ = self.stream.flush().await;

        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 12];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // リスナーをこのアドレスにバインドする
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let db = db.clone();
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};

    // データを蓄えるため `HashMap` を使う
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        //     }
        //     cmd => panic!("unimplemented {:?}", cmd),
        // };

        // クライアントへのレスポンスを書き込む
        connection.write_frame(&response).await.unwrap();
    }
}
