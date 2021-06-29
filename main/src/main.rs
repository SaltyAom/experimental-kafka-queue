use std::{
    sync::Arc,
    time::{ 
        Duration, 
        Instant
    }
};

use actix_web::{
    App, 
    HttpServer, 
    get, 
    rt,
    web::Data
};

// use tokio::time::sleep;
use tokio_stream::StreamExt;

use num_cpus;
use rand::{
    distributions::Alphanumeric, 
    Rng
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


#[derive(Clone)]
pub struct AppState {
    pub producer: Arc<FutureProducer>,
    pub receiver: flume::Receiver<String>
}

fn generate_key() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}

#[get("/")]
async fn landing(state: Data<AppState>) -> String {
    let time = Instant::now();

    let key = generate_key();

    &state.producer.send(
        FutureRecord::to("exp-queue_general-5-forth")
            .key(&key)
            .payload("Hello From Rust"),
            Duration::from_secs(5)
    )
        .await
        .expect("Unable to send message");

    let receiver = state.receiver.clone();
    let value = receiver.recv_async().await.unwrap_or("".to_owned());

    println!("Take {}", time.elapsed().as_millis());

    value
}

#[get("/status")]
async fn heartbeat() -> &'static str {
    // ? Concurrency delay check
    // sleep(Duration::from_secs(3)).await;

    "Working"
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .set("group.id", "exp-queue_general-5-forth")
        .create()
        .expect("Kafka config");

    let (tx, rx) = flume::unbounded::<String>();

    rt::spawn(async move {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", "exp-queue_general-5-back")
            .create()
            .expect("Kafka config");

        consumer
            .subscribe(&vec!["exp-queue_general-5-back".as_ref()])
            .expect("Can't subscribe");

        let mut stream = consumer.stream();

        while let Some(message) = stream.next().await {
            match message {
                Ok(message) => {
                    let result = String::from_utf8_lossy(
                        message
                            .payload()
                            .unwrap_or("Error serializing".as_bytes())
                    ).to_string();

                    tx.send_async(result).await.expect("Tx");        
                },
                Err(error) => {
                    println!("Kafka error {}", error);
                }
            }
        }
    });

    let state = AppState {
        producer: Arc::new(producer),
        receiver: rx
    };

    // ? Assume that the whole node is just Rust instance
    let mut cpus = num_cpus::get() - 1;

    if cpus < 1 {
        cpus = 1;
    }

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(state.clone()))
            .service(landing)
            .service(heartbeat)
    })
    .workers(cpus)
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
