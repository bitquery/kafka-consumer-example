mod protos;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::error::KafkaResult;
use tokio::time::{timeout, Duration};
use prost::Message as ProstMessage;
use crate::protos::dex_block_message::DexParsedBlockMessage;
use config::Config;
use serde::Deserialize;
use bs58;

#[derive(Debug, Deserialize)]
struct AuthConfig {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct Settings {
    solana: AuthConfig,
}

#[tokio::main]
async fn main() -> KafkaResult<()> {
    env_logger::init();

    // Load config from config.toml
    let settings = Config::builder()
        .add_source(config::File::with_name("config"))
        .build()
        .unwrap();

    let settings: Settings = settings.try_deserialize().unwrap();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092")
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("ssl.endpoint.identification.algorithm", "none")
        .set("sasl.mechanisms", "SCRAM-SHA-512")
        .set("sasl.username", &settings.solana.username)
        .set("sasl.password", &settings.solana.password)
        .set("group.id", &format!("{}-group-11", settings.solana.username))
        .set("fetch.message.max.bytes", "10485760")
        .create()?;

    let topics = vec!["solana.dextrades.proto"];
    consumer.subscribe(&topics)?;

    println!("Starting consumer, listening on topics: {:?}", topics);

    loop {
        match timeout(Duration::from_secs(10), consumer.recv()).await {
            Ok(msg_result) => match msg_result {
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        match DexParsedBlockMessage::decode(payload) {
                            Ok(parsed_block) => {
                                // Print block header info
                                if let Some(header) = &parsed_block.header {
                                    println!("Block: slot={:?}, timestamp={:?}", 
                                        header.slot, header.timestamp);
                                }

                                // Print transaction details
                                for dex_tx in &parsed_block.transactions {
                                    println!("--- Transaction ---");
                                    println!("  Index: {}", dex_tx.index);
                                    println!("  Signature: {}", bs58::encode(&dex_tx.signature).into_string());
                                    
                                    if let Some(status) = &dex_tx.status {
                                        println!("  Status: success={}", status.success);
                                    }

                            
                                }
                            },
                            Err(e) => {
                                eprintln!("Failed to decode DexParsedBlockMessage: {}", e);
                            }
                        }
                    }

                    consumer.commit_message(&msg, CommitMode::Async)?;
                }
                Err(e) => {
                    eprintln!("Error receiving message from Kafka: {}", e);
                }
            },
            Err(_) => {
                println!("No new messages within 10 seconds...");
            }
        }
    }
}
