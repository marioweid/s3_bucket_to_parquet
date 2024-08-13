use aws_sdk_s3::{types::Object, Client, Error};
use aws_config::SdkConfig;
use aws_types::region::Region;
use dotenv::dotenv;
use std::env;

async fn list_objects(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<Object>, Error> {
    let mut objects = Vec::new();
    let mut continuation_token = None;

    loop {
        let resp = client
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv().ok();
    let region: String = env::var("REGION").expect("REGION must be set in .env file");
    let bucket: String = env::var("BUCKET").expect("BUCKET must be set in .env file");
    let subfolder: String = env::var("SUBFOLDER").expect("SUBFOLDER must be set in .env file");
    
    // Load AWS configuration
    #[allow(deprecated)]
    let config: SdkConfig = aws_config::from_env().region(Region::new(region)).load().await;
    let client: Client = Client::new(&config);

    // List objects in the bucket subfolder
    let result: Result<Vec<Object>, Error> = list_objects(&client,  bucket.as_str(), subfolder.as_str()).await;

    match result {
        Ok(objects) => {
            for object in objects {
                if let Some(key) = &object.key {
                    println!("{}", key);
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