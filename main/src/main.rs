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

use tokio::time::sleep;

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

const TOPIC: &'static str = "exp-queue_general-5";


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
    // let time = Instant::now();
    let key = generate_key();

    &state.producer.send(
        FutureRecord::to(&format!("{}-forth", TOPIC))
            .key(&key)
            .payload("Hello From Rust"),
            Duration::from_secs(8)
    )
        .await
        .expect("Unable to send message");

    let receiver = state.receiver.clone();
    let value = receiver.recv().unwrap_or("".to_owned());

    // println!("Take {}", time.elapsed().as_millis());

    value
}

#[get("/status")]
async fn heartbeat() -> &'static str {
    // ? Concurrency delay check
    sleep(Duration::from_secs(3)).await;

    "Working"
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("linger.ms", "40")
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.ms", "50")
        .set("compression.type", "lz4")
        .set("retries", "40000")
        .set("retries", "0")
        .set("message.timeout.ms", "8000")
        .create()
        .expect("Kafka config");

    let (tx, rx) = flume::unbounded::<String>();

    rt::spawn(async move {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", &format!("{}-back", TOPIC))
            .set("queued.min.messages", "200000")
            .set("fetch.error.backoff.ms", "250")
            .set("socket.blocking.max.ms", "500")
            .create()
            .expect("Kafka config");

        consumer
            .subscribe(&vec![format!("{}-back", TOPIC).as_ref()])
            .expect("Can't subscribe");

        loop {
            let stream = consumer.recv();

            match stream.await {
                Ok(message) => {
                    let result = String::from_utf8_lossy(
                        message
                            .payload()
                            .unwrap_or("Error serializing".as_bytes())
                    ).to_string();
       
                    tx.send(result).expect("Tx not sending");
                },
                Err(error) => {
                    println!("Kafka Error: {}", error);
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
