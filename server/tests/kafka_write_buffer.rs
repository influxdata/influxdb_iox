use futures::{stream::FuturesUnordered, StreamExt};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use std::{
    array,
    convert::TryInto,
    fs::File,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[tokio::test]
async fn writes_go_to_kafka() {
    // start up kafka

    // set up a database with a write buffer pointing at kafka

    // write some points

    // check the data is in kafka

    // stop kafka
}

const TOPIC: &str = "my-topic22227"; // TODO: this should be the database name and managed by IOx
const NUM_MSGS: usize = 10; // TODO: this should go away

// TODO: these tests should be run in a separate command with TEST_INTEGRATION on in circleci

#[tokio::test]
async fn can_connect_to_kafka() {
    // TODO: should use a macro to check for TEST_INTEGRATION and make a shared ServerFixture
    // start up kafka
    Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg("../docker/ci-kafka-docker-compose.yml")
        .arg("up")
        // .arg("-d") // TODO: this needs to be uncommented
        .spawn()
        .expect("starting of docker kafka/zookeeper process");

    let mut cmd = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg("../docker/ci-kafka-docker-compose.yml")
        .arg("logs")
        // TODO: this needs to go to temp files like in influxdb2_client/tests/common/server_fixture.rs
        .spawn()
        .expect("starting of docker logs process");


    // TODO instead of producing to Kafka directly, this test should use the management api to
    // configure a write buffer pointing at 127.0.0.1:9093, should use the /write endpoint to
    // write some line protocol, then should consume the records from Kafka directly.

    // connect to kafka, produce, and consume
    let x = || {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", "127.0.0.1:9093");
        cfg
    };

    let admin_cfg = x();

    let mut producer_cfg = x();
    producer_cfg.set("message.timeout.ms", "5000");

    let mut consumer_cfg = x();
    consumer_cfg.set("session.timeout.ms", "6000");
    consumer_cfg.set("enable.auto.commit", "false");
    consumer_cfg.set("group.id", "placeholder");

    // TODO remove all the eprintln!
    eprintln!("here");

    let admin: AdminClient<DefaultClientContext> = admin_cfg.create().unwrap();
    eprintln!("created admin");
    let producer: FutureProducer = producer_cfg.create().unwrap();
    eprintln!("created producer");
    let consumer: StreamConsumer = consumer_cfg.create().unwrap();
    eprintln!("created consumer");

    let topic = NewTopic::new(TOPIC, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::default();
    admin.create_topics(&[topic], &opts).await.unwrap();

    eprintln!("created topics");

    let mut topics = TopicPartitionList::new();
    topics.add_partition(TOPIC, 0);
    topics.set_partition_offset(TOPIC, 0, Offset::Beginning).unwrap();
    consumer.assign(&topics).unwrap();

    eprintln!("Created");

    let consumer_task = tokio::spawn(async move {
        eprintln!("Consumer task starting");

        let mut counter = NUM_MSGS;

        loop {
            let p = consumer.recv().await.unwrap();
            eprintln!("Received a {:?}", p.payload().map(String::from_utf8_lossy));
            counter -= 1;
            if counter == 0 {
                break;
            }
        }
        eprintln!("Exiting Consumer");
    });

    // TODO all the producing should move to server/src/write_buffer.rs
    let producer_task = tokio::spawn(async move {
        eprintln!("Producer task starting");
        for i in 0..NUM_MSGS {
            let s = format!("hello! {}", i);
            let record = FutureRecord::to(TOPIC).key(&s).payload(&s).timestamp(now());
            match producer.send_result(record) {
                Ok(x) => match x.await.unwrap() {
                    Ok((partition, offset)) => {
                        // TODO remove all the dbg
                        dbg!(&s, partition, offset);
                    }
                    Err((e, msg)) => panic!("oh no {}, {:?}", e, msg),
                },
                Err((e, msg)) => panic!("oh no {}, {:?}", e, msg),
            }
            eprintln!("Sent {}", i);
        }
        eprintln!("exiting producer");
    });

    let mut tasks: FuturesUnordered<_> =
        array::IntoIter::new([consumer_task, producer_task]).collect();

    while let Some(t) = tasks.next().await {
        t.unwrap();
    }

    // TODO the rest of this test needs to go in the server fixture drop impl

    // stop logging
    cmd
        .kill()
        .expect("Should have been able to kill the test server");

    // stop kafka
    Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg("../docker/ci-kafka-docker-compose.yml")
        .arg("down")
        .output()
        .expect("stopping of docker kafka/zookeeper process");
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
