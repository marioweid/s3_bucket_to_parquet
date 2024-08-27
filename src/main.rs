use aws_sdk_s3::{types::Object, Client};
use aws_config::SdkConfig;
use aws_types::region::Region;
use dotenv::dotenv;
use tokio::{fs::File, io::AsyncWriteExt};
use std::{env, error::Error, path::Path};

async fn list_objects(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<Object>, aws_sdk_s3::Error> {
    let mut objects = Vec::new();
    let mut continuation_token = None;

    loop {
        let resp = client
            // .list_objects_v2()
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .set_continuation_token(continuation_token.clone())
            .send()
            .await?;

        if let Some(contents) = resp.contents {
            objects.extend(contents);
        }

        if resp.is_truncated.unwrap_or(false) {
            continuation_token = resp.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(objects)
}

async fn get_object(client: &Client, bucket: &str, obj_key: &str) -> Result<usize, Box<dyn Error>> {
    let mut file: File = File::create(obj_key).await?;
    
    let mut object = client
        .get_object()
        .bucket(bucket)
        .key(obj_key)
        .send()
        .await?;


    let mut byte_count: usize = 0_usize;
    while let Some(bytes) = object.body.try_next().await? {
        let bytes_len: usize = bytes.len();
        file.write_all(&bytes).await?;
        byte_count += bytes_len;
    }

    Ok(byte_count)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let region: String = env::var("REGION").expect("REGION must be set in .env file");
    let bucket: String = env::var("BUCKET").expect("BUCKET must be set in .env file");
    let subfolder: String = env::var("SUBFOLDER").expect("SUBFOLDER must be set in .env file");
    
    // Load AWS configuration
    #[allow(deprecated)]
    let config: SdkConfig = aws_config::from_env().region(Region::new(region)).load().await;
    let client: Client = Client::new(&config);

    // List objects in the bucket subfolder
    let result: Result<Vec<Object>, aws_sdk_s3::Error> = list_objects(&client,  bucket.as_str(), subfolder.as_str()).await;

    match result {
        Ok(objects) => {
            for object in objects {
                if let Some(key) = &object.key {
                    let file_exists: bool = Path::new(key).exists();
                    if !file_exists {
                        println!("Downloading: {} ...", key);
                        let len_bytes: usize = get_object(&client, bucket.as_str(), key).await?;
                        println!("Bytes writte: {}", len_bytes);
                    }
                }
            }
        }
        Err(e) => {
            println!("Error listing objects: {}", e);
        }
    }
    
    println!("Exit!");
    Ok(())
}