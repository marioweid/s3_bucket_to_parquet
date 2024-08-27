use aws_sdk_s3::{types::Object, Client};
use aws_config::SdkConfig;
use aws_types::region::Region;
use tokio::{fs::File, io::{AsyncWriteExt, BufReader}};
use std::{env, error::Error, path::Path};
use polars::prelude::{DataFrame, Series};
use dotenv::dotenv;
use glob::glob;
use serde_json;
mod serde_models;



#[allow(unused)]
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

#[allow(unused)]
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

fn json_structs_to_dataframe(data: Vec<serde_models::JsonStruct>) -> Result<DataFrame> {
    let ids: Vec<u64> = data.iter().map(|s| s.col_1).collect();
    let parents: Vec<u64> = data.iter().map(|s| s.col_2).collect();
    let threads: Vec<u64> = data.iter().map(|s| s.col_3).collect();
    let forums: Vec<&str> = data.iter().map(|s| s.col_4.as_str()).collect();
    let messages: Vec<&str> = data.iter().map(|s| s.col_5.as_str()).collect();
    let created_ats: Vec<&str> = data.iter().map(|s| s.col_6.as_str()).collect();
    let is_hatespeeches: Vec<Option<bool>> = data.iter().map(|s| s.col_7).collect();
    let hs_scores: Vec<f64> = data.iter().map(|s| s.col_8).collect();

    let df = DataFrame::new(vec![
        Series::new("id", ids),
        Series::new("parent", parents),
        Series::new("thread", threads),
        Series::new("forum", forums),
        Series::new("message", messages),
        Series::new("created_at", created_ats),
        Series::new("is_hatespeech", is_hatespeeches),
        Series::new("hs_score", hs_scores),
    ])?;

    Ok(df)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    // AWS STUFF, commented out to safe s3 requests
    // let region: String = env::var("REGION").expect("REGION must be set in .env file");
    // let bucket: String = env::var("BUCKET").expect("BUCKET must be set in .env file");
    // let subfolder: String = env::var("SUBFOLDER").expect("SUBFOLDER must be set in .env file");
    
    // // Load AWS configuration
    // #[allow(deprecated)]
    // let config: SdkConfig = aws_config::from_env().region(Region::new(region)).load().await;
    // let client: Client = Client::new(&config);
    // List objects in the bucket subfolder
    // let result: Result<Vec<Object>, aws_sdk_s3::Error> = list_objects(&client,  bucket.as_str(), subfolder.as_str()).await;
    // match result {
    //     Ok(objects) => {
    //         for object in objects {
    //             if let Some(key) = &object.key {
    //                 let file_exists: bool = Path::new(key).exists();
    //                 if !file_exists {
    //                     println!("Downloading: {} ...", key);
    //                     let len_bytes: usize = get_object(&client, bucket.as_str(), key).await?;
    //                     println!("Bytes writte: {}", len_bytes);
    //                 }
    //             }
    //         }
    //     }
    //     Err(e) => {
    //         println!("Error listing objects: {}", e);
    //     }
    // }

    let subfolder: String = env::var("SUBFOLDER").expect("SUBFOLDER must be set in .env file");
    let mut json_structs: Vec<serde_models::JsonStruct>= Vec::new();
    for entry in glob(format!("{}/*.json", subfolder).as_str()).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => {
                let file = std::fs::File::open(path)?;
                let reader = std::io::BufReader::new(file);
                let mut file_data: Vec<serde_models::JsonStruct> = serde_json::from_reader(reader)?;
                json_structs.append(&mut file_data);
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }

    let df = json_structs_to_dataframe(all_data)?;
    // Print the DataFrame
    println!("{:?}", df);



    println!("{:?}", json_structs);
    println!("Exit!");
    Ok(())
}