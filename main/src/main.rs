use std::{
    sync::Arc,
    time::{ 
        Duration, 
        Instant
    }
};

use actix::{ Actor, Message as ActixMessage, Context, Handler };
use actix_web::{
    App, 
    HttpServer, 
    get, 
    rt,
    web::Data
};

use futures::TryStreamExt;
use tokio::time::sleep;

use num_cpus;
use rand::{
    distributions::Alphanumeric, 
    Rng
};

use rdkafka::{
    ClientConfig, 
    Message as KafkaMessage, 
    consumer::{
        Consumer, 
        StreamConsumer
    }, 
    producer::{
        BaseProducer, 
        BaseRecord
    }
};

const TOPIC: &'static str = "exp-queue_general-5";

#[derive(r#ActixMessage)]
#[rtype(result = "Result<String, std::io::Error>")]
pub struct ReceiverActorInput {
    receiver: flume::Receiver<String>
}

pub struct ReceiverActor;

impl Actor for ReceiverActor {
    type Context = Context<Self>;
}

impl Handler<ReceiverActorInput> for ReceiverActor {
    type Result = Result<String, std::io::Error>;

    fn handle(&mut self, msg: ReceiverActorInput, _ctx: &mut Context<Self>) -> Self::Result {
        Ok(
            msg
                .receiver
                .recv()
                .unwrap_or(
                    "".to_owned()
                )
            )
    }
}

#[derive(r#ActixMessage)]
#[rtype(result = "Result<(), std::io::Error>")]
pub struct ProducerActorInput {
    producer: Arc<BaseProducer>,
    message: &'static str
}

pub struct ProducerActor;

impl Actor for ProducerActor {
    type Context = Context<Self>;
}

impl Handler<ProducerActorInput> for ProducerActor {
    type Result = Result<(), std::io::Error>;

    fn handle(&mut self, msg: ProducerActorInput, _ctx: &mut Context<Self>) -> Self::Result {
        let producer = msg.producer.clone();
        let message = msg.message;

        let key = generate_key();

        producer
            .send(
                BaseRecord::to(&format!("{}-forth", TOPIC))
                    .key(&key)
                    .payload(message)
            )
            .expect("Unable to send message");

        Ok(())
    }
}

#[derive(Clone)]
pub struct AppState {
    pub producer: Arc<BaseProducer>,
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
    // let t1 = Instant::now();

    let producer = state.producer.clone();
    let receiver = state.receiver.clone();

    let producer_actor = ProducerActor.start();
    let receiver_actor = ReceiverActor.start();

    producer_actor.send(ProducerActorInput {
        producer,
        message: "Hello from Rust"
    })
        .await
        .unwrap()
        .unwrap();

    let value = receiver_actor.send(ReceiverActorInput {
        receiver
    })
        .await
        .unwrap()
        .unwrap();

    // println!("Process take {} ms", t1.elapsed().as_millis());

    value
}

#[get("/status")]
async fn heartbeat() -> &'static str {
    // ? Concurrency delay check
    sleep(Duration::from_secs(1)).await;

    "Working"
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // ? Assume that the whole node is just Rust instance
    let mut cpus = num_cpus::get() / 2 - 1;

    if cpus < 1 {
        cpus = 1;
    }

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("linger.ms", "15")
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.ms", "15")
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

        consumer
            .stream()
            .try_for_each_concurrent(
                cpus,
                |message| {
                    let txx = tx.clone();

                    async move {
                        let result = String::from_utf8_lossy(
                            message
                            .payload()
                            .unwrap_or("Error serializing".as_bytes())
                        ).to_string();

                        txx.send(result).expect("Tx not sending");

                        Ok(())
                    }

                }
            )
            .await
            .expect("Error reading stream");
    });

    let state = AppState {
        producer: Arc::new(producer),
        receiver: rx
    };

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(state.clone()))
            .service(landing)
            .service(heartbeat)
    })
    .workers(6)
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
