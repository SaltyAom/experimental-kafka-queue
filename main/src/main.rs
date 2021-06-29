use std::{sync::Arc, time::{ Duration, Instant, SystemTime, UNIX_EPOCH }};
use tokio_stream::StreamExt;
use rand::{distributions::Alphanumeric, Rng};

use actix_web::{
    App, HttpServer, get,
    web::Data
};

use rdkafka::{
    ClientConfig, 
    Message, 
    consumer::{
        Consumer, 
        StreamConsumer
    }, 
    producer::{
        FutureProducer, 
        FutureRecord
    }
};

fn generate_key() -> String {
    let key: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect();

    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .unwrap();

    format!("{}-{}", since_the_epoch.as_millis(), key)
}

#[get("/")]
async fn landing(state: Data<AppState>) -> String {
    let t = Instant::now();

    let key = generate_key();

    &state.producer.send(
        FutureRecord::to("exp-queue_general-forth")
            .key(&key)
            .payload("Hello From Rust"),
            Duration::from_secs(5)
    )
        .await
        .expect("Unable to send message");

    println!("Send take {}", t.elapsed().as_millis());

    let mut stream = state.consumer.stream();
    let mut value = String::from("");

    while let Ok(message) = stream.next().await.unwrap() {
        let result = String::from_utf8_lossy(
            message
                .payload()
                .unwrap_or("Error serializing".as_bytes())
        ).to_string();

        if message.key().unwrap() == key.as_bytes() {
            value = result.to_owned();

            break;
        }
    }

    println!("Take {}", t.elapsed().as_millis());

    value
}

#[derive(Clone)]
pub struct AppState {
    pub producer: Arc<FutureProducer>,
    pub consumer: Arc<StreamConsumer>
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .set("group.id", "exp-queue_general-forth")
        .create()
        .expect("Kafka config");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "exp-queue_general-back")
        .create()
        .expect("Kafka config");

    consumer
        .subscribe(&vec!["exp-queue_general-back".as_ref()])
        .expect("Can't subscribe");

    let state = AppState {
        producer: Arc::new(producer),
        consumer: Arc::new(consumer)
    };

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(state.clone()))
            .service(landing)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
