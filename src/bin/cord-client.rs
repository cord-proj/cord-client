use clap::{
    crate_authors, crate_description, crate_name, crate_version, App, AppSettings, Arg, SubCommand,
};
use cord_client::{errors::*, Client};
use env_logger;
use futures::{future, future::join_all, StreamExt, TryFutureExt, TryStreamExt};
use log::error;
use tokio::io::{self, AsyncBufReadExt};

#[tokio::main]
async fn main() -> Result<()> {
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

    // Connect to the broker
    let conn = Client::connect(sock_addr).await?;

    if let Some(matches) = matches.subcommand_matches("pub") {
        let provides: Vec<&str> = matches.values_of("provide").unwrap().collect();

        publish(conn, provides.into_iter().map(|s| s.to_owned()).collect()).await?;
    } else if let Some(matches) = matches.subcommand_matches("sub") {
        let subscribes: Vec<&str> = matches.values_of("subscribe").unwrap().collect();
        subscribe(conn, subscribes.into_iter().map(|s| s.to_owned()).collect()).await?;
    }

    Ok(())
}

async fn publish(mut conn: Client, provides: Vec<String>) -> Result<()> {
    // Send provides
    for namespace in provides {
        conn.provide(namespace.into()).await?;
    }

    print!("\x1B[2J\x1B[H");
    println!("Start typing to create an event, then press enter to send it to the broker.");
    println!("Use the format: NAMESPACE=VALUE");
    println!();

    // Send events
    io::BufReader::new(io::stdin())
        .lines()
        .map_err(|e| ErrorKind::Msg(e.to_string()).into())
        .try_fold(conn, |mut conn, event| async move {
            let parts: Vec<&str> = event.split('=').collect();

            // Check that the input has a namespace and a value
            if parts.len() == 2 {
                conn.event(parts[0].into(), parts[1]).await?;
            } else {
                error!("Events must be in format NAMESPACE=VALUE");
            }

            Ok(conn)
        })
        .map_ok(|_| ())
        .await
}

async fn subscribe(mut conn: Client, subscribes: Vec<String>) -> Result<()> {
    let mut futs = Vec::new();

    for namespace in subscribes {
        // We can't chain this to the for_each as `conn` is mutably borrowed
        // asynchronously. Perhaps someone smarter than me can find away around that?
        let s = conn.subscribe(namespace.into()).await?;

        futs.push(s.for_each(|(p, s)| {
            println!("{:?}: {}", p, s);
            future::ready(())
        }));
    }

    join_all(futs).await;
    Ok(())
}
