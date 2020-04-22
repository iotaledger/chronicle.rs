use indicatif::{
    ProgressBar,
    ProgressStyle,
};
use sha2::{
    Digest,
    Sha256,
};
use std::{
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
};

static PROGRESS_STEP: u64 = 1000000;

// TODO: error handling
#[allow(dead_code)]
/// Read the file from file_path w/ progress calculation
async fn read_file(file_name: &str) -> Result<(), Box<dyn Error>> {
    let mut file = File::open(file_name).await?;
    // Get the total file length
    let total_size = file.seek(SeekFrom::End(0)).await?;

    // Back to the starting location of file
    file.seek(SeekFrom::Start(0)).await?;
    let reader = BufReader::new(&mut file);
    let mut lines = reader.lines().map(|res| res.unwrap());
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
    while let Some(line) = lines.next().await {
        let v: Vec<&str> = line.split(',').collect();

        // TODO: use worker to insert the tx to database
        let _tx = v[0];

        // Add 1 for the endline
        cur_pos += line.len() as u64 + 1;
        if cur_pos > next_progress {
            next_progress += PROGRESS_STEP;
            pb.set_position(cur_pos);
        }
    }
    // Complete the progress, minus 1 due to no endline in the last line
    pb.set_position(cur_pos - 1);
    pb.finish_with_message(&format!("{} is processed succesfully.", file_name));
    Ok(())
}

#[allow(dead_code)]
/// Download the file from url and return the checksum by SHA256
async fn download_file(url: &str) -> Result<String, Box<dyn Error>> {
    let mut response = reqwest::get(url).await?;
    let file_name = Path::new(url).file_name().unwrap();
    {
        let mut file = tokio::fs::File::create(file_name).await?;
        // let len = response.content_length().unwrap();
        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk).await?;
        }
    }
    // Reopen the file from the disk and calculate checksum
    let mut sha256 = Sha256::new();
    let mut std_file = tokio::fs::File::open(file_name).await?.into_std().await;
    std::io::copy(&mut std_file, &mut sha256)?;
    let hash = sha256.result();
    Ok(format!("{:x}", hash))
}
