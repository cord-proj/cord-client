//! Cord is a data streaming platform for composing, aggregating and distributing
//! arbitrary streams. It uses a publish-subscribe model that allows multiple publishers
//! to share their streams via a Cord Broker. Subscribers can then compose custom sinks
//! using a regex-like pattern to access realtime data based on their individual
//! requirements.
//!
//! To interact with a Broker, we use this library:
//!
//! # Examples
//!
//! ```no_run
//! use cord_client::Client;
//!# use cord_client::errors::Error;
//!
//!# async fn test() -> Result<(), Error> {
//! let mut conn = Client::connect("127.0.0.1:7101".parse().unwrap()).await?;
//!
//! // Tell the broker we're going to provide the namespace /users
//! conn.provide("/users".into()).await?;
//!
//! // Start publishing events...
//! conn.event("/users/mark".into(), "Mark has joined").await?;
//!
//!# Ok(())
//!# }
//! ```
//!
//! # Cord CLI
//! For one-off interactions with a Broker, there is also the Cord CLI, which is
//! available via Cargo:
//!
//! ```no_run
//! $ cargo install cord-client
//! $ cord-client sub /namespaces
//! ```
//!
//! For more usage, check the usage guidelines on [cord-proj.org](https://cord-proj.org).

pub mod errors;

use cord_message::{errors::Error as MessageError, Codec, Message, Pattern};
use errors::{Error, ErrorKind, Result};
use futures::{
    future::{self, try_select},
    stream::SplitSink,
    Sink, SinkExt, Stream, StreamExt, TryStreamExt,
};
use futures_locks_pre::Mutex;
use retain_mut::RetainMut;
use tokio::{net::TcpStream, sync::mpsc, sync::oneshot};
use tokio_util::codec::Framed;

use std::{
    collections::HashMap,
    convert::Into,
    net::SocketAddr,
    ops::Drop,
    pin::Pin,
    result,
    sync::Arc,
    task::{Context, Poll},
};

/// The `Client` type alias defines the `Sink` type for communicating with a Broker.
///
/// This type alias should be used for normal operation in favour of consuming
/// `ClientConn` directly. This type is instantiated using
/// [`Client::connect`](struct.ClientConn.html#method.connect).
///
/// The reason for this alias' existence is so that the `Sink` can be overridden for
/// testing.
pub type Client = ClientConn<SplitSink<Framed<TcpStream, Codec>, Message>>;

/// The `ClientConn` manages the connection between you and a Cord Broker.
///
/// Using a generic `Sink` (`S`) allows us to build mocks for testing. However for normal
/// use, it is strongly recommended to use the type alias, [`Client`](type.Client.html).
pub struct ClientConn<S> {
    sink: S,
    inner: Arc<Inner>,
}

/// A `Subscriber` encapsulates a stream of events for a subscribed namespace. It is
/// created by [`Client::subscribe()`](struct.Client.html#method.subscribe).
///
/// # Examples
///
/// ```
///# use cord_client::{errors::Result, Client};
///# use futures::{future, StreamExt, TryFutureExt};
///
///# async fn test() -> Result<()> {
/// let mut conn = Client::connect("127.0.0.1:7101".parse().unwrap()).await?;
/// conn.subscribe("/users/".into())
///     .and_then(|sub| async {
///         sub.for_each(|(namespace, data)| {
///             // Handle the message...
///             dbg!("Received the namespace '{}' with data: {}", namespace, data);
///             future::ready(())
///         })
///         .await;
///         Ok(())
///     })
///     .await
///# }
/// ```
pub struct Subscriber {
    receiver: mpsc::Receiver<Message>,
    _inner: Arc<Inner>,
}

struct Inner {
    receivers: Mutex<HashMap<Pattern, Vec<mpsc::Sender<Message>>>>,
    detonator: Option<oneshot::Sender<()>>,
}

impl<S> ClientConn<S>
where
    S: Sink<Message, Error = MessageError> + Unpin,
{
    /// Connect to a broker
    pub async fn connect(addr: SocketAddr) -> Result<Client> {
        // This channel is used to shutdown the stream listener when the Client is dropped
        let (det_tx, det_rx) = oneshot::channel();

        // Connect to the broker
        let sock = TcpStream::connect(&addr).await?;

        // Wrap socket in message codec
        let framed = Framed::new(sock, Codec::default());
        let (sink, stream) = framed.split();

        // Setup the receivers map
        let receivers = Mutex::new(HashMap::new());
        let receivers_c = receivers.clone();

        // Route the codec's stream to receivers
        let router = Box::pin(
            stream
                .map_err(|e| Error::from_kind(ErrorKind::Message(e)))
                .try_fold(receivers_c, |recv, message| async move {
                    route(&recv, message).await;
                    Ok(recv)
                }),
        );

        tokio::spawn(try_select(router, det_rx));

        Ok(ClientConn {
            sink,
            inner: Arc::new(Inner {
                receivers,
                detonator: Some(det_tx),
            }),
        })
    }

    /// Inform the broker that you will be providing a new namespace
    pub async fn provide(&mut self, namespace: Pattern) -> Result<()> {
        self.sink
            .send(Message::Provide(namespace))
            .await
            .map_err(|e| ErrorKind::Message(e).into())
    }

    /// Inform the broker that you will no longer be providing a namespace
    pub async fn revoke(&mut self, namespace: Pattern) -> Result<()> {
        self.sink
            .send(Message::Revoke(namespace))
            .await
            .map_err(|e| ErrorKind::Message(e).into())
    }

    /// Subscribe to another provider's namespace
    ///
    /// # Examples
    ///
    /// ```
    ///# use cord_client::{errors::Result, Client};
    ///# use futures::{future, StreamExt, TryFutureExt};
    ///
    ///# async fn test() -> Result<()> {
    /// let mut conn = Client::connect("127.0.0.1:7101".parse().unwrap()).await?;
    /// conn.subscribe("/users/".into())
    ///     .and_then(|sub| async {
    ///         sub.for_each(|(namespace, data)| {
    ///             // Handle the message...
    ///             dbg!("Received the namespace '{}' with data: {}", namespace, data);
    ///             future::ready(())
    ///         })
    ///         .await;
    ///         Ok(())
    ///     })
    ///     .await
    ///# }
    /// ```
    pub async fn subscribe(&mut self, namespace: Pattern) -> Result<Subscriber> {
        let namespace_c = namespace.clone();
        self.sink.send(Message::Subscribe(namespace)).await?;

        let (tx, rx) = mpsc::channel(10);
        self.inner
            .receivers
            .with(move |mut guard| {
                (*guard)
                    .entry(namespace_c)
                    .or_insert_with(Vec::new)
                    .push(tx);
                future::ready(())
            })
            .await;
        Ok(Subscriber {
            receiver: rx,
            _inner: self.inner.clone(),
        })
    }

    /// Unsubscribe from another provider's namespace
    pub async fn unsubscribe(&mut self, namespace: Pattern) -> Result<()> {
        let namespace_c = namespace.clone();
        self.sink.send(Message::Unsubscribe(namespace)).await?;

        self.inner
            .receivers
            .with(move |mut guard| {
                (*guard).remove(&namespace_c);
                future::ready(())
            })
            .await;
        Ok(())
    }

    /// Publish an event to your subscribers
    pub async fn event<Str: Into<String>>(&mut self, namespace: Pattern, data: Str) -> Result<()> {
        self.sink
            .send(Message::Event(namespace, data.into()))
            .await
            .map_err(|e| ErrorKind::Message(e).into())
    }
}

impl<E, S, T> Sink<T> for ClientConn<S>
where
    S: Sink<T, Error = E>,
    E: Into<Error>,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<result::Result<(), Self::Error>> {
        unsafe { Pin::map_unchecked_mut(self, |x| &mut x.sink) }
            .poll_ready(cx)
            .map_err(|e| e.into())
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> result::Result<(), Self::Error> {
        unsafe { Pin::map_unchecked_mut(self, |x| &mut x.sink) }
            .start_send(item)
            .map_err(|e| e.into())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<result::Result<(), Self::Error>> {
        unsafe { Pin::map_unchecked_mut(self, |x| &mut x.sink) }
            .poll_flush(cx)
            .map_err(|e| e.into())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<result::Result<(), Self::Error>> {
        unsafe { Pin::map_unchecked_mut(self, |x| &mut x.sink) }
            .poll_close(cx)
            .map_err(|e| e.into())
    }
}

impl Stream for Subscriber {
    type Item = (Pattern, String);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        unsafe { Pin::map_unchecked_mut(self, |x| &mut x.receiver) }
            .poll_next(cx)
            .map(|opt_msg| match opt_msg {
                Some(Message::Event(pattern, data)) => Some((pattern, data)),
                None => None,
                _ => unreachable!(),
            })
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Ignore any error from the channel as an error indicates that the other side
        // has already terminated.
        let _ = self
            .detonator
            .take()
            .expect("Inner has already been terminated")
            .send(());
    }
}

async fn route(receivers: &Mutex<HashMap<Pattern, Vec<mpsc::Sender<Message>>>>, message: Message) {
    receivers
        .with(move |mut guard| {
            // Remove any subscribers that have no senders left
            (*guard).retain(|namespace, senders| {
                // We assume that all messages will be Events. If this changes, we will
                // need to store a Message, not a pattern.
                if namespace.contains(message.namespace()) {
                    // Remove any senders that give errors when attempting to send
                    senders.retain_mut(|tx| tx.try_send(message.clone()).is_ok());
                }

                // So long as we have senders, keep the subscriber
                !senders.is_empty()
            });

            future::ready(())
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    use cord_message::errors::ErrorKind as MessageErrorKind;

    // Using Futures channel instead of Tokio as Tokio's channel implementation is
    // missing a Sink implementation
    use futures::channel::mpsc::{unbounded, UnboundedReceiver};

    struct ForwardStream(Vec<Message>);

    impl Stream for ForwardStream {
        type Item = Result<Message>;

        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.0.pop().map(Ok))
        }
    }

    #[allow(clippy::type_complexity)]
    fn setup_client() -> (
        ClientConn<impl Sink<Message, Error = MessageError>>,
        UnboundedReceiver<Message>,
        Mutex<HashMap<Pattern, Vec<mpsc::Sender<Message>>>>,
    ) {
        let (tx, rx) = unbounded();
        let (det_tx, _) = oneshot::channel();
        let receivers = Mutex::new(HashMap::new());

        (
            ClientConn {
                sink: tx.sink_map_err(|e| MessageErrorKind::Msg(format!("{}", e)).into()),
                inner: Arc::new(Inner {
                    receivers: receivers.clone(),
                    detonator: Some(det_tx),
                }),
            },
            rx,
            receivers,
        )
    }

    #[tokio::test]
    async fn test_forward() {
        let (client, rx, _) = setup_client();

        let data_stream = ForwardStream(vec![
            Message::Event("/a".into(), "b".into()),
            Message::Provide("/a".into()),
        ]);
        data_stream.forward(client).await.unwrap();

        // We check these messages in reverse order (i.e. Provide, then Event), because
        // our budget DIY stream sends them in reverse order.
        let (item, rx) = rx.into_future().await;
        assert_eq!(item, Some(Message::Provide("/a".into())));

        let (item, _) = rx.into_future().await;
        assert_eq!(item, Some(Message::Event("/a".into(), "b".into())));
    }

    #[tokio::test]
    async fn test_provide() {
        let (mut client, rx, _) = setup_client();

        client.provide("/a/b".into()).await.unwrap();
        assert_eq!(
            rx.into_future().await.0.unwrap(),
            Message::Provide("/a/b".into())
        );
    }

    #[tokio::test]
    async fn test_revoke() {
        let (mut client, rx, _) = setup_client();

        client.revoke("/a/b".into()).await.unwrap();
        assert_eq!(
            rx.into_future().await.0.unwrap(),
            Message::Revoke("/a/b".into())
        );
    }

    #[tokio::test]
    async fn test_subscribe() {
        let (mut client, rx, receivers) = setup_client();

        client.subscribe("/a/b".into()).await.unwrap();

        // Check that the mock broker (`rx`) has received our message
        assert_eq!(
            rx.into_future().await.0.unwrap(),
            Message::Subscribe("/a/b".into())
        );

        // Check that the `receivers` routing table has been updated
        let guard = receivers.lock().await;
        assert!((*guard).contains_key(&"/a/b".into()));
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let (mut client, rx, receivers) = setup_client();

        receivers
            .with(|mut guard| {
                (*guard).insert("/a/b".into(), Vec::new());
                future::ready(())
            })
            .await;

        client.unsubscribe("/a/b".into()).await.unwrap();

        // Check that the mock broker (`rx`) has received our message
        assert_eq!(
            rx.into_future().await.0.unwrap(),
            Message::Unsubscribe("/a/b".into())
        );

        // Check that the `receivers` routing table has been updated
        let guard = receivers.lock().await;
        assert!((*guard).is_empty());
    }

    #[tokio::test]
    async fn test_event() {
        let (mut client, rx, _) = setup_client();

        client.event("/a/b".into(), "moo").await.unwrap();
        assert_eq!(
            rx.into_future().await.0.unwrap(),
            Message::Event("/a/b".into(), "moo".into())
        );
    }

    #[tokio::test]
    async fn test_route() {
        let (tx, rx) = mpsc::channel(10);
        let receivers = Mutex::new(HashMap::new());

        receivers
            .with(|mut guard| {
                (*guard).insert("/a/b".into(), vec![tx]);
                future::ready(())
            })
            .await;

        let event_msg = Message::Event("/a/b".into(), "Moo!".into());
        let event_msg_c = event_msg.clone();

        route(&receivers, event_msg).await;

        // Check that the subscriber has received our message
        assert_eq!(rx.into_future().await.0.unwrap(), event_msg_c);

        let guard = receivers.lock().await;
        assert!((*guard).contains_key(&"/a/b".into()));
    }

    #[tokio::test]
    async fn test_route_norecv() {
        let (tx, _) = mpsc::channel(10);
        let receivers = Mutex::new(HashMap::new());

        receivers
            .with(|mut guard| {
                (*guard).insert("/a/b".into(), vec![tx]);
                future::ready(())
            })
            .await;

        route(&receivers, Message::Event("/a/b".into(), "Moo!".into())).await;

        // Check that the unused receiver has been removed
        let guard = receivers.lock().await;
        assert!((*guard).is_empty());
    }
}
