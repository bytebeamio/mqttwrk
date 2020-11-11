use crate::ConsoleConfig;
use rumqttc::{qos, Event, Outgoing, Client, MqttOptions};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::collections::VecDeque;
use std::{io, thread};
use rand::{thread_rng, Rng};
use structopt::StructOpt;
use flume::{bounded, Receiver};

struct Console {
    client: Client,
    events_rx: Receiver<Event>
}

impl Console {
    fn new(options: MqttOptions) -> Console {
        let (client, connection) = Client::new(options, 10);
        let (events_tx, events_rx) = bounded(10);

        thread::spawn(move || {
            let mut connection = connection;
            for notification in connection.iter() {
                match notification {
                    Ok(event) => {
                        if let Err(e) = events_tx.send(event) {
                            println!("{:?}", e);
                            break;
                        }
                    },
                    Err(e) => {
                        println!("{:?}", e);
                        continue
                    }
                }
            }
        });

        Console { client, events_rx }
    }

    fn publish(&mut self, topic: &str, q: u8, payload: String) {
        self
            .client
            .publish(topic, qos(q).unwrap(), false, payload)
            .unwrap();

        let pkid = loop {
            match self.events_rx.recv() {
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

    fn subscribe(&mut self, topic: &str, q: u8) {
        self
            .client
            .subscribe(topic, qos(q).unwrap())
            .unwrap();

        loop {
            match self.events_rx.recv() {
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
    let id = format!("mqttwrkconsole@{}", thread_rng().gen_range(1, 1_000_000));
    let mut console = Console::new(config.options(&id)?);

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
                    }
                    Command::Subscribe(subscribe) => {
                        console.subscribe(&subscribe.filter, subscribe.qos);
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
