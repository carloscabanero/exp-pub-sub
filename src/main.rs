use cloud_pubsub::{error, Topic};
use cloud_pubsub::{Client, EncodedMessage, FromPubSubMessage, Subscription};
use serde::{Deserializer};
use serde_derive::{Deserialize, Serialize};
//use std::future::BoxFuture;
use futures::future::BoxFuture;
use std::sync::Arc;
use std::time::Duration;
use tokio::{signal, task, time};
use tracing::{debug, error, info};

#[derive(Deserialize)]
struct Config {
    topic: String,
    subscription: String,
    gcp_credentials: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct MachineStatsPacket {
    id: u64,
    secs: u32,
}

impl FromPubSubMessage for MachineStatsPacket {
    fn from(message: EncodedMessage) -> Result<Self, error::Error> {
        match message.decode() {
            Ok(bytes) => {
                serde_json::from_slice::<MachineStatsPacket>(&bytes).map_err(error::Error::from)
            }
            Err(e) => Err(error::Error::from(e)),
        }
    }
}

// https://stackoverflow.com/questions/41081240/idiomatic-callbacks-in-rust
struct Worker<T> where
    T: FromPubSubMessage + Send + 'static + std::fmt::Debug
{
// NOTE The Fn has a struct associated and we do not know the size when we send it, so it may require an
// Arc or Box to store it.
    subscription: Arc<Subscription>,
    work: Arc<dyn Fn(T) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
}

impl <T> Worker<T> where
    T: FromPubSubMessage + Send + 'static + std::fmt::Debug
{
    // We may probably wrap the client ourselves too, so the "token" and stuff like that can be taken out.
    // Then the worker would just receive "work".
    fn new(subscription: Arc<Subscription>, work: impl Fn(T) -> BoxFuture<'static, ()>+ Sync + Send + 'static) -> Self
    {
        Worker {
            subscription: subscription,
            work: Arc::new(work)
        }
    }
    // We could refactor to not store the function and just run it (maybe). Or use scoped threads.
    fn run(self) {
        task::spawn(async move {
            while self.subscription.client().is_running() {
                match self.subscription.get_messages::<T>().await {
                    Ok(messages) => {
                        for (result, ack_id) in messages {
                            match result {
                                Ok(message) => {
                                    //info!("recieved {:?}", message);
                                    let subscription = Arc::clone(&self.subscription);
                                    // T needs to be 'static just to help the compiler deal with it.
                                    // Not an issue as it is moved.
                                    let work = Arc::clone(&self.work);
                                    task::spawn(async move {
                                        println!("{:?}", message);
                                        work(message).await;
                                        subscription.acknowledge_messages(vec![ack_id]).await;
                                    });
                                }
                                Err(e) => error!("Failed converting to UpdatePacket: {}", e),
                            }
                        }
                    }
                    Err(e) => error!("Failed to pull PubSub messages: {}", e),
                }
            }
            debug!("No longer pulling");
        });

    }
}

// We need an Arc here as the Subscription/Client needs to continuously renew the token in the background. (!!)
// All this complexity should have been hidden as part of whatever the subscription does with the client.
// I guess we can wrap it.
fn schedule_pubsub_pull<T: FromPubSubMessage + Send + 'static, F>(subscription: Arc<Subscription>, work: F) where F: Fn(T) + Send + 'static + Copy {
    task::spawn(async move {
        while subscription.client().is_running() {
            match subscription.get_messages::<T>().await {
                Ok(messages) => {
                    for (result, ack_id) in messages {
                        match result {
                            Ok(message) => {
                                //info!("recieved {:?}", message);
                                let subscription = Arc::clone(&subscription);
                                task::spawn(async move {
                                    work(message);
                                    subscription.acknowledge_messages(vec![ack_id]).await;
                                });
                            }
                            Err(e) => error!("Failed converting to UpdatePacket: {}", e),
                        }
                    }
                }
                Err(e) => error!("Failed to pull PubSub messages: {}", e),
            }
        }
        debug!("No longer pulling");
    });
}

fn schedule_usage_metering(topic: Topic) {
    let dur = Duration::from_secs(2);
    let mut interval = time::interval(dur);
    task::spawn(async move {
        loop {
            interval.tick().await;
            let p = MachineStatsPacket {
                id: 1,
                secs: dur.as_secs() as _,
            };
            let m = EncodedMessage::new(&p, None);
            match topic.publish_message(m).await {
                Ok(response) => {
                    info!("{:?}", response);
                }
                Err(err) => error!("{}", err),
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let parsed_env = envy::from_env::<Config>();
    if let Err(e) = parsed_env {
        error!("ENV is not valid: {}", e);
        std::process::exit(1);
    }
    let config = parsed_env.unwrap();

    let pubsub = match Client::new(config.gcp_credentials).await {
        Err(e) => panic!("Failed to initialize pubsub: {}", e),
        Ok(mut client) => {
            if let Err(e) = client.refresh_token().await {
                panic!("Failed to get token: {}", e);

            } else {
                info!("Got fresh token");
            }
            Arc::new(client)
        }
    };

    pubsub.spawn_token_renew(Duration::from_secs(15 * 60));

    let topic = pubsub.topic(config.topic);

    schedule_usage_metering(topic);

    // TODO I would wrap in our own client, then the subscription or topics could be taken care of
    // from a single place, by keeping track of who is contacting it.
    // Then on close we would just wait for everything to wrap up.
    let subscription = pubsub.subscribe(config.subscription);
    
    debug!("Subscribed to topic with: {}", subscription.name);
    let sub = Arc::new(subscription);
    // TODO I would spin the task here instead of at the function
    // schedule_pubsub_pull(sub.clone(), |msg: MachineStatsPacket| {
    //     println!("{}", msg.id);
    // });

    // I could not return a Future itself, because the size did not work. So how about boxing it...
    // This is similar to what the async-trait does, but it allows us to call it here.
    // https://stackoverflow.com/questions/58173711/how-can-i-store-an-async-function-in-a-struct-and-call-it-from-a-struct-instance
    // https://www.reddit.com/r/rust/comments/fcgun1/reason_for_using_boxpin/
    let w = Worker::new(sub.clone(), |msg: MachineStatsPacket| {
        Box::pin(async move {            
            println!("RECEIVED! {:?}", msg); 
        })
    });
    
    w.run();

    signal::ctrl_c().await?;
    debug!("Cleaning up");
    // They are using variables to indicate the status.
    // Stop is just changing the state the stopped. And then we are WAITING
    // on a refernce to be zero to actually exit.
    // With go you would wait on channels.
    pubsub.stop();
    debug!("Waiting for current Pull to finish....");
    while Arc::strong_count(&sub) > 1 {}
    Ok(())
}

// TODO Test method here to just add something to the queue.
// TODO Test method to try the "message" itself.

// The worker could be returned as part of a function, passing the "context" of DO or Redis to it.
