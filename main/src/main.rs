use std::{sync::{Arc, Mutex, mpsc::{ Receiver, channel }}, time::{ Duration, Instant, SystemTime, UNIX_EPOCH }, mem};
use tokio_stream::StreamExt;
use tokio::sync::{ futures };
use rand::{distributions::Alphanumeric, Rng};

use actix_web::{
    App, HttpServer, get, rt,
    web::Data
};

use rdkafka::{ClientConfig, Message, consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer}, producer::{
        FutureProducer, 
        FutureRecord
    }};

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

    let a = state.stream.lock().unwrap();

    let b = a.recv().unwrap_or("".to_owned());

    println!("{}", b);

    println!("Take {}", t.elapsed().as_millis());

    // value
    "Hello World".to_owned()
}

#[derive(Clone)]
pub struct AppState {
    pub producer: Arc<FutureProducer>,
    pub stream: Arc<Mutex<Receiver<String>>>
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

    let (tx, rx) = channel::<String>();

    rt::spawn(async move {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", "exp-queue_general-back")
            .create()
            .expect("Kafka config");

        consumer
            .subscribe(&vec!["exp-queue_general-back".as_ref()])
            .expect("Can't subscribe");

        let mut a = consumer.stream();

        println!("Hi");

        while let Ok(message) = a.next().await.unwrap() {
            println!("Got somethin");

            let result = String::from_utf8_lossy(
                message
                    .payload()
                    .unwrap_or("Error serializing".as_bytes())
            ).to_string();

            println!("Got {}", result);
            
            tx.send(result).expect("Tx");
        }

        println!("Lol bye");
    });

    let state = AppState {
        producer: Arc::new(producer),
        stream: Arc::new(Mutex::new(rx))
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
