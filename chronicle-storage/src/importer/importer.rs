use crate::{
    ring::ring::Ring,
    stage::reporter,
    worker::{
        self,
        Worker,
    },
};
use bee_ternary::{
    t1b1::T1B1Buf,
    TritBuf,
    TryteBuf,
};
use chronicle_common::actor;
use chronicle_cql::{
    compression::compression::UNCOMPRESSED,
    frame::{
        consistency::Consistency,
        decoder::{
            Decoder,
            Frame,
        },
        header::{
            Header,
            IGNORE,
        },
        query::Query,
        queryflags::{
            SKIP_METADATA,
            VALUES,
        },
    },
};
use indicatif::{
    ProgressBar,
    ProgressStyle,
};
use sha2::{
    Digest,
    Sha256,
};
use std::{
    convert::TryFrom,
    error::Error,
    io::SeekFrom,
    path::Path,
};
use tokio::{
    fs::File,
    io::{
        AsyncBufReadExt,
        BufReader,
    },
    prelude::*,
    stream::StreamExt,
    sync::mpsc,
};

type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
#[derive(Debug)]
pub struct InsertTransactionsFromFileId(Sender);

actor!(InsertTransactionsFromFileBuilder {
    filepath: String,
    statement: String
});

impl InsertTransactionsFromFileBuilder {
    pub fn build(self) -> InsertTransactionsFromFile {
        InsertTransactionsFromFile {
            filepath: self.filepath.unwrap(),
            statement: self.statement.unwrap(),
        }
    }
}

pub struct InsertTransactionsFromFile {
    filepath: String,
    statement: String,
}

pub enum Event {
    Response {
        giveload: Vec<u8>,
        pid: Box<InsertTransactionsFromFileId>,
    },
    Error {
        kind: worker::Error,
        pid: Box<InsertTransactionsFromFileId>,
    },
}

static PROGRESS_STEP: u64 = 1000000;

impl Worker for InsertTransactionsFromFileId {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        // to enable reusable self(Sender), we will do unsafe trick
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let pid = Box::from_raw(raw);
            let event = Event::Response { giveload, pid };
            // now we can use raw to send self through itself.
            let _ = (*raw).0.send(event);
        }
    }
    fn send_error(self: Box<Self>, kind: worker::Error) {
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let pid = Box::from_raw(raw);
            let event = Event::Error { kind, pid };
            // now we can use raw to send itself through itself.
            let _ = (*raw).0.send(event);
        }
    }
}

impl InsertTransactionsFromFile {
    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Event>();
        let mut worker = Box::new(InsertTransactionsFromFileId(tx));
        let mut file = File::open(&self.filepath).await?;
        // Get the total file length
        let total_size = file.seek(SeekFrom::End(0)).await?;
        // Init the current position
        let mut cur_pos = 0;
        // The progress bar in CLI
        let pb = ProgressBar::new(total_size);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) \n {msg}",
                )
                .progress_chars("#>-"),
        );

        // Back to the starting location of file
        file.seek(SeekFrom::Start(0)).await?;
        let reader = BufReader::new(&mut file);
        let mut lines = reader.lines().map(|res| res.unwrap());

        // Show the progress when every 1MB are processed
        let mut next_progress = PROGRESS_STEP;
        while let Some(line) = lines.next().await {
            let v: Vec<&str> = line.split(',').collect();
            worker = Self::process(v[0], v[1], &self.statement, worker, &mut rx).await;
            // Add 1 for the endline
            cur_pos += line.len() as u64 + 1;
            if cur_pos > next_progress {
                next_progress += PROGRESS_STEP;
                pb.set_position(cur_pos);
            }
        }
        // Complete the progress, minus 1 due to no endline in the last line
        pb.set_position(cur_pos - 1);
        pb.finish_with_message(&format!("{} is processed succesfully.", self.filepath));

        Ok(())
    }

    async fn process(
        hash: &str,
        rawtx: &str,
        statement: &str,
        worker: Box<InsertTransactionsFromFileId>,
        rx: &mut Receiver,
    ) -> Box<InsertTransactionsFromFileId> {
        let request = reporter::Event::Request {
            payload: Self::query(hash, rawtx, statement),
            worker,
        };
        // use random token till murmur3 hash function algo impl is ready
        // send_local_random_replica will select random replica for token.
        Ring::send_local_random_replica(rand::random::<i64>(), request);
        match rx.recv().await.unwrap() {
            Event::Response { giveload, pid } => {
                let decoder = Decoder::new(giveload, UNCOMPRESSED);
                if decoder.is_void() {
                    // Nothing to do
                } else {
                    // TODO: Add retry mechanism
                }
                return pid;
            }
            Event::Error { kind: _, pid } => {
                // do nothing as the value is already null,
                // still we can apply other retry strategies
                return pid;
            }
        }
    }
    fn query(hash: &str, rawtx: &str, statement: &str) -> Vec<u8> {
        let Query(payload) = Query::new()
            .version()
            .flags(IGNORE)
            .stream(0)
            .opcode()
            .length()
            .statement(statement)
            .consistency(Consistency::One)
            .query_flags(SKIP_METADATA | VALUES)
            .value_count(17) // the total value count
            .value(&hash[..])
            .value(&rawtx[..2187]) // PAYLOAD
            .value(&rawtx[2187..2268]) // ADDRESS
            .value(&rawtx[2268..2295]) // VALUE
            .value(&rawtx[2295..2322]) // OBSOLETE_TAG
            .value(&rawtx[2322..2331]) // TIMESTAMP
            .value(&rawtx[2331..2340]) // CURRENT_IDX
            .value(&rawtx[2340..2349]) // LAST_IDX
            .value(&rawtx[2349..2430]) // BUNDLE_HASH
            .value(&rawtx[2430..2511]) // TRUNK
            .value(&rawtx[2511..2592]) // BRANCH
            .value(&rawtx[2592..2619]) // TAG
            .value(&rawtx[2619..2628]) // ATCH_TIMESTAMP
            .value(&rawtx[2628..2637]) // ATCH_TIMESTAMP_LOWER
            .value(&rawtx[2637..2646]) // ATCH_TIMESTAMP_UPPER
            .value(&rawtx[2646..2673]) // Nonce
            .unset_value() // not-set value for milestone
            .build(UNCOMPRESSED);
        payload
    }
}

#[allow(dead_code)]
// This conversion is for milestone insertion
fn str_to_i64(slice: &str) -> i64 {
    let trytes = TryteBuf::try_from_str(slice);
    let trit_buf: TritBuf<T1B1Buf> = trytes.unwrap().as_trits().encode();
    i64::try_from(trit_buf).unwrap()
}

#[allow(dead_code)]
/// Download the file from url and return the checksum by SHA256
async fn download_file(url: &str) -> Result<String, Box<dyn Error>> {
    let mut response = reqwest::get(url).await?;
    let file_name = Path::new(url).file_name().unwrap();
    {
        let mut file = tokio::fs::File::create(file_name).await?;
        let total_size = response.content_length().unwrap();
        let mut cur_pos = 0;

        // The progress bar in CLI
        let pb = ProgressBar::new(total_size);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) \n {msg}",
                )
                .progress_chars("#>-"),
        );
        // Show the progress when every 1MB are processed
        let mut next_progress = PROGRESS_STEP;

        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk).await?;
            cur_pos += chunk.len() as u64;
            if cur_pos > next_progress {
                next_progress += PROGRESS_STEP;
                pb.set_position(cur_pos);
            }
        }
        pb.set_position(cur_pos);
        pb.finish_with_message(&format!("{} is downloaded succesfully.", url));
    }
    // Reopen the file from the disk and calculate checksum
    let mut sha256 = Sha256::new();
    let mut std_file = tokio::fs::File::open(file_name).await?.into_std().await;
    std::io::copy(&mut std_file, &mut sha256)?;
    let hash = sha256.result();
    Ok(format!("{:x}", hash))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_download_file() {
        let hash = download_file("https://sh.rustup.rs").await.unwrap();
        assert_eq!(hash.len(), 64);
    }

    // // Uncommet the following codes for quick test
    // #[tokio::test]
    // async fn test_load_file() {
    //     let _ = InsertTransactionsFromFileBuilder::new()
    //         .filepath("YOUR_FILE_PATH".to_string())
    //         .statement(INSERT_TX_QUERY.to_string())
    //         .build()
    //         .run()
    //         .await
    //         .unwrap();
    // }
}
