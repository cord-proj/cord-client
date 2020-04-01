use clap::{
    crate_authors, crate_description, crate_name, crate_version, App, AppSettings, Arg, SubCommand,
};
use cord_client::{errors::*, Conn};
use env_logger;
use futures::{future::join_all, Future, Stream};
use log::error;
use std::{io::BufReader, net::SocketAddr};
use tokio::io;

fn main() -> Result<()> {
    env_logger::init();

    let matches = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("address")
                .short("a")
                .long("address")
                .value_name("SERVER")
                .help("The IP address to connect to (defaults to 127.0.0.1)")
                .takes_value(true)
                .default_value("127.0.0.1"),
        )
        .arg(
            Arg::with_name("port")
                .short("P")
                .long("port")
                .value_name("PORT")
                .help("The port number to connect to (defaults to 7101)")
                .takes_value(true)
                .default_value("7101"),
        )
        .subcommand(
            SubCommand::with_name("pub")
                .about("Publish events to a Cord broker")
                .arg(
                    Arg::with_name("provide")
                        .help("A namespace pattern that you will provide events for")
                        .multiple(true)
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("sub")
                .about("Subscribe to events from a Cord broker")
                .arg(
                    Arg::with_name("subscribe")
                        .help("A namespace to subscribe to")
                        .multiple(true)
                        .required(true),
                ),
        )
        .get_matches();

    // Create socket address
    // `value_of().unwrap()` is safe as a default value will always be available
    let port = matches.value_of("port").unwrap().trim();
    // `value_of().unwrap()` is safe as a default value will always be available
    let addr = matches.value_of("address").unwrap().trim();
    let sock_addr = format!("{}:{}", addr, port)
        .parse()
        .expect("Invalid broker address");

    if let Some(matches) = matches.subcommand_matches("pub") {
        let provides: Vec<&str> = matches.values_of("provide").unwrap().collect();

        publish(
            sock_addr,
            provides.into_iter().map(|s| s.to_owned()).collect(),
        );
    } else if let Some(matches) = matches.subcommand_matches("sub") {
        let subscribes: Vec<&str> = matches.values_of("subscribe").unwrap().collect();
        subscribe(
            sock_addr,
            subscribes.into_iter().map(|s| s.to_owned()).collect(),
        );
    }

    Ok(())
}

fn publish(sock_addr: SocketAddr, provides: Vec<String>) {
    let conn = Conn::new(sock_addr).and_then(move |mut conn| {
        // Send provides
        provides.into_iter().for_each(|namespace| {
            conn.provide(namespace.into())
                .expect("Could not send PROVIDE message")
        });

        print!("\x1B[2J\x1B[H");
        println!("Start typing to create an event, then press enter to send it to the broker.");
        println!("Use the format: NAMESPACE=VALUE");
        println!();

        // Send events
        let stdin = io::lines(BufReader::new(io::stdin()))
            .map_err(|e| ErrorKind::Msg(e.to_string()).into());
        stdin.for_each(move |event| {
            let parts: Vec<&str> = event.split('=').collect();

            // Check that the input has a namespace and a value
            if parts.len() == 2 {
                conn.event(parts[0].into(), parts[1])
                    .expect("Could not send EVENT message");
            } else {
                error!("Events must be in format NAMESPACE=VALUE");
            }

            Ok(())
        })
    });

    tokio::run(conn.map(|_| ()).map_err(|e| error!("{}", e)));
}

fn subscribe(sock_addr: SocketAddr, subscribes: Vec<String>) {
    let conn = Conn::new(sock_addr).and_then(move |mut conn| {
        let mut futs = Vec::new();

        for namespace in subscribes {
            futs.push(
                conn.subscribe(namespace.into())
                    .expect("Could not send SUBSCRIBE message")
                    .for_each(|(p, s)| {
                        println!("{:?}: {}", p, s);
                        Ok(())
                    }),
            );
        }

        join_all(futs).map(|_| ())
    });

    tokio::run(conn.map(|_| ()).map_err(|e| error!("{}", e)));
}
