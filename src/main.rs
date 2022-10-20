#![allow(unused)] // silence unused warnings while exploring (to comment out)

// cargo watch -q -c -x 'run -q'

use anyhow::{anyhow, bail, Context, Result};
use std::env;
use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use aws_sdk_s3::{ByteStream, Client, config, Credentials, Region};
use tokio_stream::StreamExt;

// AWS creds
const AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const BUCKET_NAME: &str = "rust-aws-cdk-demo";
const REGION: &str = "eu-west-2";

#[tokio::main]
async fn main() -> Result<()> {
    let client = get_aws_client(REGION)?;
    let keys = list_keys(&client, BUCKET_NAME).await?;
    println!("List:\n{}", keys.join("\n"));
    let path = Path::new("src/main.rs");
    upload_file(&client, BUCKET_NAME, path).await?;
    println!("Uploaded file {}", path.display());

    let dir = Path::new("./downloads/");
    let key = "level1/file.txt";
    download_file(&client, BUCKET_NAME, key, dir).await?;
    Ok(())
}

async fn download_file(client: &Client, bucket_name: &str, key: &str, dir: &Path) -> Result<()> {
    if !dir.is_dir() {
        bail!("Path {} is not a directory", dir.display());
    }

    // create file path and parent dir(s)
    let file_path = dir.join(key);
    let parent_dir = file_path
        .parent()
        .ok_or_else(|| anyhow!("Invalid parent dir for {:?}", file_path))?;

    if !parent_dir.exists(){
        create_dir_all(parent_dir)?;
    }

    // build aws request
    let request = client.get_object().bucket(bucket_name).key(key);

    // execute request
    let response = request.send().await?;

    // stream result to file
    let mut data: ByteStream = response.body;
    let file = File::create(&file_path)?;
    let mut buf_writer = BufWriter::new(file);
    while let Some(bytes) = data.try_next().await? {
        buf_writer.write(&bytes)?;
    }
    buf_writer.flush()?;

    println!("Downloaded {} from S3 into {}", key, file_path.display());
    Ok(())
}

async fn upload_file(client: &Client, bucket_name: &str, path: &Path) -> Result<()> {
    // validate if file exist
    if !path.exists() {
        bail!("Path {} does not exists", path.display());
    }
    let key = path.to_str().ok_or_else(|| anyhow!("Invalid path {path:?}"))?;

    // prepare aws request
    let body = ByteStream::from_path(&path).await?;
    let content_type = mime_guess::from_path(&path).first_or_octet_stream().to_string();

    // build aws request
    let request = client
        .put_object()
        .bucket(bucket_name)
        .key(key)
        .body(body)
        .content_type(content_type);

    // execute
    request.send().await?;
    Ok(())

}

async fn list_keys(client: &Client, bucket_name: &str) -> Result<Vec<String>> {
    // build aws request
    let request = client.list_objects_v2().prefix("").bucket(bucket_name);

    // execute request
    let response = request.send().await?;
    // collect
    let keys = response.contents.unwrap_or_default();

    let keys = keys
        .iter()
        .filter_map(|o| o.key.as_ref())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    Ok(keys)
}

fn get_aws_client(region: &str) -> Result<Client> {
    let key_id = env::var(AWS_ACCESS_KEY_ID).context("Missing AWS_ACCESS_KEY_ID")?;
    let access_key = env::var(AWS_SECRET_ACCESS_KEY).context("Missing AWS_SECRET_ACCESS_KEY")?;

    let credentials = Credentials::new(key_id, access_key, None, None, "load-from-env");
    // build the aws client
    let region = Region::new(region.to_string());
    let conf_builder = config::Builder::new().region(region).credentials_provider(credentials);
    let conf = conf_builder.build();

    // build aws client
    let client = Client::from_conf(conf);
    Ok(client)
}

