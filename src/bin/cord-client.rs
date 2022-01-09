use clap::{AppSettings, Parser, Subcommand};
use cord_client::{errors::*, Client};
use futures::{future, future::join_all, StreamExt};
use log::error;
use std::net::{AddrParseError, IpAddr, Ipv4Addr};
use tokio::io::{self, AsyncBufReadExt};

// Parse string inputs into IP addresses
fn parse_ip(input: &str) -> ::std::result::Result<IpAddr, AddrParseError> {
    input.parse()
}

#[derive(Parser, Debug)]
#[clap(about, version, author)]
#[clap(global_setting(AppSettings::UseLongFormatForHelpSubcommand))]
struct Args {
    /// The IP address to connect to - defaults to 127.0.0.1
    #[clap(short, long, default_value_t = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), parse(try_from_str = parse_ip))]
    address: IpAddr,

    /// The port number to connect to - defaults to 7101
    #[clap(short, long, default_value_t = 7101, parse(try_from_str))]
    port: u16,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Publish events to a Cord broker
    Pub { provides: Vec<String> },

    /// Subscribe to events from a Cord broker
    Sub { subscribes: Vec<String> },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    // Connect to the broker
    let client = Client::connect((args.address, args.port)).await?;

    match args.command {
        Commands::Pub { provides } => publish(client, provides).await?,
        Commands::Sub { subscribes } => subscribe(client, subscribes).await?,
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
    while let Some(event) = io::BufReader::new(io::stdin()).lines().next_line().await? {
        let parts: Vec<&str> = event.split('=').collect();

        // Check that the input has a namespace and a value
        if parts.len() == 2 {
            conn.event(parts[0].into(), parts[1]).await?;
        } else {
            error!("Events must be in format NAMESPACE=VALUE");
        }
    }

    Ok(())
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
