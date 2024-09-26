use aws_sdk_s3::Client;
use tokio::{fs::File, io::AsyncWriteExt};
use std::{env, error::Error};
use polars::prelude::{DataFrame, NamedFrom, Series, ParquetWriter};
use dotenv::dotenv;
use glob::glob;
use serde_json;
mod serde_models;



// #[allow(unused)]
// async fn list_objects(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<Object>, aws_sdk_s3::Error> {
//     let mut objects = Vec::new();
//     let mut continuation_token = None;

//     loop {
//         let resp = client
//             // .list_objects_v2()
//             .list_objects_v2()
//             .bucket(bucket)
//             .prefix(prefix)
//             .set_continuation_token(continuation_token.clone())
//             .send()
//             .await?;

//         if let Some(contents) = resp.contents {
//             objects.extend(contents);
//         }

//         if resp.is_truncated.unwrap_or(false) {
//             continuation_token = resp.next_continuation_token;
//         } else {
//             break;
//         }
//     }

//     Ok(objects)
// }

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

fn json_structs_to_dataframe(data: Vec<serde_models::JsonStruct>) -> Result<DataFrame, Box<dyn Error>> {
    let ids: Vec<u64> = data.iter().map(|s| s.col_1).collect();
    let parents: Vec<Option<u64>> = data.iter().map(|s| s.col_2).collect();
    let threads: Vec<u64> = data.iter().map(|s| s.col_3).collect();
    let forums: Vec<&str> = data.iter().map(|s| s.col_4.as_str()).collect();
    let messages: Vec<&str> = data.iter().map(|s| s.col_5.as_str()).collect();
    let created_ats: Vec<&str> = data.iter().map(|s| s.col_6.as_str()).collect();
    let is_hatespeeches: Vec<Option<bool>> = data.iter().map(|s| s.col_7).collect();
    let hs_scores: Vec<Option<f64>> = data.iter().map(|s| s.col_8).collect();

    let df: DataFrame = DataFrame::new(vec![
        Series::new("id".into(), ids),
        Series::new("parent".into(), parents),
        Series::new("thread".into(), threads),
        Series::new("forum".into(), forums),
        Series::new("message".into(), messages),
        Series::new("created_at".into(), created_ats),
        Series::new("is_hatespeech".into(), is_hatespeeches),
        Series::new("hs_score".into(), hs_scores),
    ])?;

    // let s0 = Series::new("a".into(), [1 , 2, 3]);
    // let s1: Series = Series::new("temp".into(), [22.1, 19.9, 7.]);
    // let df: DataFrame = DataFrame::new(vec![s0, s1])?;

    Ok(df)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    use std::time::Instant;
    let now = Instant::now();

    dotenv().ok();
    // AWS STUFF, commented out to safe s3 requests
    // let region: String = env::var("REGION").expect("REGION must be set in .env file");
    // let bucket: String = env::var("BUCKET").expect("BUCKET must be set in .env file");
    // let subfolder: String = env::var("SUBFOLDER").expect("SUBFOLDER must be set in .env file");
    
    // Load AWS configuration
    // #[allow(deprecated)]
    // let config: SdkConfig = aws_config::from_env().region(Region::new(region)).load().await;
    // let client: Client = Client::new(&config);
    // // List objects in the bucket subfolder
    // let result: Result<Vec<Object>, aws_sdk_s3::Error> = list_objects(&client,  bucket.as_str(), subfolder.as_str()).await;
    // match result {
    //     Ok(objects) => {
    //         for object in objects {
    //             if let Some(key) = &object.key {
    //                 let file_exists: bool = Path::new(key).exists();
    //                 if !file_exists {#[allow(unused)]
    //                     println!("Bytes writte: {}", len_bytes);
    //                 }
    //             }
    //         }
    //     }
    //     Err(e) => {
    //         println!("Error listing objects: {}", e);
    //     }
    // }

    // Load JSON from Folder
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

    let mut df: DataFrame = json_structs_to_dataframe(json_structs)?;
    // Print the DataFrame
    let mut file: std::fs::File = std::fs::File::create("out/rust.parquet").unwrap();
    ParquetWriter::new(&mut file).finish(&mut df).unwrap();



    // println!("{:?}", json_structs);
    println!("Exit!");
    
    let elapsed: std::time::Duration = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    Ok(())
}