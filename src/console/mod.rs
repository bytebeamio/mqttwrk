use crate::link::Link;
use crate::ConsoleConfig;
use rumqttc::{qos, Event, Outgoing};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::collections::VecDeque;
use std::io;
use structopt::StructOpt;

struct Console {
    link: Link,
}

impl Console {
    fn new(link: Link) -> Console {
        Console { link }
    }

    async fn publish(&mut self, topic: &str, q: u8, payload: String) {
        self.link
            .client
            .publish(topic, qos(q).unwrap(), false, payload)
            .await
            .unwrap();

        let pkid = loop {
            match self.link.eventloop.poll().await {
                Ok(Event::Outgoing(Outgoing::Publish(pkid))) => break pkid,
                Ok(_) => continue,
                Err(e) => {
                    println!("{:?}", e);
                    return;
                }
            }
        };

        println!("Sent = {}", pkid);
    }

    async fn subscribe(&mut self, topic: &str, q: u8) {
        self.link
            .client
            .subscribe(topic, qos(q).unwrap())
            .await
            .unwrap();

        loop {
            match self.link.eventloop.poll().await {
                Ok(Event::Incoming(event)) => {
                    println!("{:?}", event);
                }
                Ok(_) => continue,
                Err(e) => {
                    println!("{:?}", e);
                    return;
                }
            }
        }
    }
}

#[derive(Debug, StructOpt)]
enum Command {
    Publish(Publish),
    Subscribe(Subscribe),
}

#[derive(Debug, StructOpt)]
struct Publish {
    /// topic to publish to
    #[structopt(short = "t", long = "topic")]
    topic: String,
    /// payload
    #[structopt(short = "p", long = "payload")]
    payload: String,
    /// qos
    #[structopt(short = "q", long = "qos", default_value = "1")]
    qos: u8,
}

#[derive(Debug, StructOpt)]
struct Subscribe {
    /// filter to subscribe to
    #[structopt(short = "t", long = "topic")]
    filter: String,
    /// qos
    #[structopt(short = "q", long = "qos", default_value = "1")]
    qos: u8,
}

pub(crate) async fn start(config: ConsoleConfig) -> io::Result<()> {
    let mut console = Console::new(
        Link::new(
            "mqttwrkconsole",
            &config.server,
            config.port,
            config.keep_alive,
            config.max_inflight,
            config.conn_timeout,
            config.ca_file.clone(),
            config.client_cert.clone(),
            config.client_key.clone(),
        )
        .unwrap(),
    );

    let mut rl = Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                let mut command: VecDeque<_> = line.split(" ").collect();
                command.push_front("->");

                let command = match Command::from_iter_safe(command) {
                    Ok(command) => command,
                    Err(e) => {
                        println!("{}", e.message);
                        continue;
                    }
                };

                match command {
                    Command::Publish(publish) => {
                        console
                            .publish(&publish.topic, publish.qos, publish.payload)
                            .await;
                    }
                    Command::Subscribe(subscribe) => {
                        console.subscribe(&subscribe.filter, subscribe.qos).await;
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    rl.save_history("history.txt").unwrap();
    Ok(())
}
