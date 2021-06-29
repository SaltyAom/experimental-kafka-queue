use std::{
    sync::{
        Arc, 
    }, 
    time::{ 
        Duration, 
        Instant,
    }
};

use actix_web::{
    App, HttpServer, get, rt,
    web::Data
};

use tokio_stream::StreamExt;
use num_cpus;

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

#[get("/")]
async fn landing(state: Data<AppState>) -> String {
    let time = Instant::now();

    &state.producer.send(
        FutureRecord::to("exp-queue_general-4-forth")
            .key("test")
            .payload("Hello From Rust"),
            Duration::from_secs(5)
    )
        .await
        .expect("Unable to send message");

    let stream = state.stream.clone();
    let value = stream.recv_async().await.unwrap_or("".to_owned());

    println!("Take {}", time.elapsed().as_millis());

    value
}

#[derive(Clone)]
pub struct AppState {
    pub producer: Arc<FutureProducer>,
    pub stream: flume::Receiver<String>
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .set("group.id", "exp-queue_general-4-forth")
        .create()
        .expect("Kafka config");

    let (tx, rx) = flume::unbounded::<String>();

    rt::spawn(async move {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", "exp-queue_general-4-back")
            .create()
            .expect("Kafka config");

        consumer
            .subscribe(&vec!["exp-queue_general-4-back".as_ref()])
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
        
                    // println!("Got {}", result);

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
        stream: rx
    };

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(state.clone()))
            .service(landing)
    })
    .workers(num_cpus::get() - 1)
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
