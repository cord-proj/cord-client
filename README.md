# DEPRECATED

This project is deprecated in favour of the
[Selium project](https://github.com/seliumlabs/selium), which is actively maintained.

# Cord Client

![CI Code Testing and Linting](https://github.com/cord-proj/cord-client/workflows/CI%20Code%20Testing%20and%20Linting/badge.svg)
![CI Security Audit on Push](https://github.com/cord-proj/cord-client/workflows/CI%20Security%20Audit%20on%20Push/badge.svg)

Cord is a data streaming platform for composing, aggregating and distributing arbitrary
streams. The Client crate provides user interfaces for publishing and subscribing to
Cord Brokers.

## Usage

First, start a new [Cord Broker](https://github.com/cord-proj/cord-broker):

**Docker**

    $ docker run -d -p 7101:7101 --rm cordproj/cord-broker:0

**Cargo**

    $ cargo install cord-broker
    $ cord-broker &

Next, use the Client to interact with the Broker. You can implement Cord within your own
project using the [Client library](https://docs.rs/cord-client), however the easiest way
to get started is by using the Client CLI.

Subscribe to a namespace:

**Docker**

    $ docker run --rm cordproj/cord-client:0 -a <broker_addr> sub /names

**Cargo**

    $ cargo install cord-client
    $ cord-client sub /namespaces

Publish to this namespace:

**Docker**

    $ docker run -it --rm cordproj/cord-client:0 -a <broker_addr> pub /names
    Start typing to create an event, then press enter to send it to the broker.
    Use the format: NAMESPACE=VALUE

    /names/first=Daz

**Cargo**

    $ cord-client pub /names
    Start typing to create an event, then press enter to send it to the broker.
    Use the format: NAMESPACE=VALUE

    /names/first=Daz
