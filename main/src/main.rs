use std::{
    sync::{
        Arc, 
        Mutex, 
        mpsc::{ 
            Receiver, 
            channel 
        }
    }, 
    time::{ 
        Duration, 
        // Instant,
    }
};
use tokio_stream::StreamExt;

use actix_web::{
    App, HttpServer, get, rt,
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

#[get("/")]
async fn landing(state: Data<AppState>) -> String {
    // let t = Instant::now();

    &state.producer.send(
        FutureRecord::to("exp-queue_general-4-forth")
            .key("test")
            .payload("Hello From Rust"),
            Duration::from_secs(5)
    )
        .await
        .expect("Unable to send message");

    let stream = state.stream.lock().unwrap();
    let value = stream.recv().unwrap_or("".to_owned());

    // println!("Take {}", t.elapsed().as_millis());

    value
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
        .set("group.id", "exp-queue_general-4-forth")
        .create()
        .expect("Kafka config");

    let (tx, rx) = channel::<String>();

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
        
                    println!("Got {}", result);

                    tx.send(result).expect("Tx");        
                },
                Err(error) => {
                    println!("Kafka error {}", error);
                }
            }
        }
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
