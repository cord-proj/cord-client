pub mod errors;

use cord_message::{Codec, Message, Pattern};
use errors::{Error, ErrorKind, Result};
use futures::{
    compat::Compat01As03, future::try_select, stream::SplitSink, Sink, SinkExt, Stream, StreamExt,
    TryStreamExt,
};
use futures_locks::Mutex;
use retain_mut::RetainMut;
use tokio::{net::TcpStream, sync::mpsc, sync::oneshot};
use tokio_util::codec::Framed;

use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::Drop,
    pin::Pin,
    result,
    sync::Arc,
    task::{Context, Poll},
};

/// A `Client` is used to connect to and communicate with a broker.
///
/// # Examples
///
/// ```no_run
/// use cord_client::{errors::Result, Client};
/// use futures::{future, Future};
/// use tokio;
///
///# async fn test() -> Result<()> {
/// let mut conn = Client::connect("127.0.0.1:7101".parse().unwrap()).await?;
///
/// // Tell the broker we're going to provide the namespace /users
/// conn.provide("/users".into()).await?;
///
/// // Start publishing events...
/// conn.event("/users/mark".into(), "Mark has joined").await?;
///
///# Ok(())
///# }
/// ```
pub struct Client {
    sink: SplitSink<Framed<TcpStream, Codec>, Message>,
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

impl Client {
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
                    route(&recv, message).await?;
                    Ok(recv)
                }),
        );

        tokio::spawn(try_select(router, det_rx));

        Ok(Client {
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
        tokio::spawn(Compat01As03::new(
            self.inner
                .receivers
                .with(move |mut guard| {
                    (*guard)
                        .entry(namespace_c)
                        .or_insert_with(Vec::new)
                        .push(tx);
                    Result::Ok(()) // Leave as Result::Ok for Error type elision
                })
                .expect("The default executor has shut down"),
        ));
        Ok(Subscriber {
            receiver: rx,
            _inner: self.inner.clone(),
        })
    }

    /// Unsubscribe from another provider's namespace
    pub async fn unsubscribe(&mut self, namespace: Pattern) -> Result<()> {
        let namespace_c = namespace.clone();
        self.sink.send(Message::Unsubscribe(namespace)).await?;

        tokio::spawn(Compat01As03::new(
            self.inner
                .receivers
                .with(move |mut guard| {
                    (*guard).remove(&namespace_c);
                    Result::Ok(()) // Leave as Result::Ok for Error type elision
                })
                .expect("The default executor has shut down"),
        ));
        Ok(())
    }

    /// Publish an event to your subscribers
    pub async fn event<S: Into<String>>(&mut self, namespace: Pattern, data: S) -> Result<()> {
        self.sink
            .send(Message::Event(namespace, data.into()))
            .await
            .map_err(|e| ErrorKind::Message(e).into())
    }
}

impl Sink<Message> for Client {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<result::Result<(), Self::Error>> {
        unsafe { Pin::map_unchecked_mut(self, |x| &mut x.sink) }
            .poll_ready(cx)
            .map_err(|e| ErrorKind::Message(e).into())
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> result::Result<(), Self::Error> {
        unsafe { Pin::map_unchecked_mut(self, |x| &mut x.sink) }
            .start_send(item)
            .map_err(|e| ErrorKind::Message(e).into())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<result::Result<(), Self::Error>> {
        unsafe { Pin::map_unchecked_mut(self, |x| &mut x.sink) }
            .poll_flush(cx)
            .map_err(|e| ErrorKind::Message(e).into())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<result::Result<(), Self::Error>> {
        unsafe { Pin::map_unchecked_mut(self, |x| &mut x.sink) }
            .poll_close(cx)
            .map_err(|e| ErrorKind::Message(e).into())
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
        self.detonator.take().unwrap().send(()).unwrap();
    }
}

async fn route(
    receivers: &Mutex<HashMap<Pattern, Vec<mpsc::Sender<Message>>>>,
    message: Message,
) -> Result<()> {
    Compat01As03::new(
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

                Result::Ok(())
            })
            .expect("The default executor has shut down"),
    )
    .await
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use futures::future;
//
//     struct ForwardStream(Vec<Message>);
//     impl Stream for ForwardStream {
//         type Item = Message;
//
//         fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
//             Poll::Ready(self.0.pop())
//         }
//     }
//
//     fn setup_conn() -> Client {
//         let (det_tx, _) = oneshot::channel();
//
//         Client {
//             sink: tx,
//             inner: Arc::new(Inner {
//                 receivers: Mutex::new(HashMap::new()),
//                 detonator: Some(det_tx),
//             }),
//         }
//     }
//
//     #[tokio::test]
//     async fn test_forward() {
//         let conn = setup_conn();
//
//         let data_stream = ForwardStream(vec![
//             Message::Event("/a".into(), "b".into()),
//             Message::Provide("/a".into()),
//         ]);
//         data_stream.forward(conn).await.unwrap();
//
//         // We check these messages in reverse order (i.e. Provide, then Event), because
//         // our budget DIY stream sends them in reverse order.
//         let (item, rx) = rx.into_future().wait().unwrap();
//         assert_eq!(item, Some(Message::Provide("/a".into())));
//
//         let (item, _) = rx.into_future().wait().unwrap();
//         assert_eq!(item, Some(Message::Event("/a".into(), "b".into())));
//     }
//
//     #[test]
//     fn test_provide() {
//         let (tx, rx) = mpsc::unbounded_channel();
//         let (det_tx, _det_rx) = oneshot::channel();
//
//         let mut conn = Client {
//             sender: tx,
//             inner: Arc::new(Inner {
//                 receivers: Mutex::new(HashMap::new()),
//                 detonator: Some(det_tx),
//             }),
//         };
//
//         conn.provide("/a/b".into()).unwrap();
//         assert_eq!(
//             rx.into_future().wait().unwrap().0.unwrap(),
//             Message::Provide("/a/b".into())
//         );
//     }
//
//     #[test]
//     fn test_revoke() {
//         let (tx, rx) = mpsc::unbounded_channel();
//         let (det_tx, _det_rx) = oneshot::channel();
//
//         let mut conn = Client {
//             sender: tx,
//             inner: Arc::new(Inner {
//                 receivers: Mutex::new(HashMap::new()),
//                 detonator: Some(det_tx),
//             }),
//         };
//
//         conn.revoke("/a/b".into()).unwrap();
//         assert_eq!(
//             rx.into_future().wait().unwrap().0.unwrap(),
//             Message::Revoke("/a/b".into())
//         );
//     }
//
//     #[test]
//     fn test_subscribe() {
//         let (tx, rx) = mpsc::unbounded_channel();
//         let (det_tx, _det_rx) = oneshot::channel();
//
//         let receivers: Mutex<HashMap<Pattern, Vec<mpsc::Sender<Message>>>> =
//             Mutex::new(HashMap::new());
//
//         let mut conn = Client {
//             sender: tx,
//             inner: Arc::new(Inner {
//                 receivers: receivers.clone(),
//                 detonator: Some(det_tx),
//             }),
//         };
//
//         // All this extra fluff around future::lazy() is necessary to ensure that there
//         // is an active executor when the fn calls Mutex::with().
//         tokio::run(future::lazy(move || {
//             conn.subscribe("/a/b".into()).unwrap();
//             Ok(())
//         }));
//         assert_eq!(
//             rx.into_future().wait().unwrap().0.unwrap(),
//             Message::Subscribe("/a/b".into())
//         );
//         assert!(receivers.try_unwrap().unwrap().contains_key(&"/a/b".into()));
//     }
//
//     #[test]
//     fn test_unsubscribe() {
//         let (tx, rx) = mpsc::unbounded_channel();
//         let (det_tx, _det_rx) = oneshot::channel();
//
//         let mut receivers: HashMap<Pattern, Vec<mpsc::Sender<Message>>> = HashMap::new();
//         receivers.insert("/a/b".into(), Vec::new());
//         let receivers = Mutex::new(receivers);
//
//         let mut conn = Client {
//             sender: tx,
//             inner: Arc::new(Inner {
//                 receivers: receivers.clone(),
//                 detonator: Some(det_tx),
//             }),
//         };
//
//         // All this extra fluff around future::lazy() is necessary to ensure that there
//         // is an active executor when the fn calls Mutex::with().
//         tokio::run(future::lazy(move || {
//             conn.unsubscribe("/a/b".into()).unwrap();
//             Ok(())
//         }));
//         assert_eq!(
//             rx.into_future().wait().unwrap().0.unwrap(),
//             Message::Unsubscribe("/a/b".into())
//         );
//         assert!(receivers.try_unwrap().unwrap().is_empty());
//     }
//
//     #[test]
//     fn test_event() {
//         let (tx, rx) = mpsc::unbounded_channel();
//         let (det_tx, _det_rx) = oneshot::channel();
//
//         let mut conn = Client {
//             sender: tx,
//             inner: Arc::new(Inner {
//                 receivers: Mutex::new(HashMap::new()),
//                 detonator: Some(det_tx),
//             }),
//         };
//
//         conn.event("/a/b".into(), "moo").unwrap();
//         assert_eq!(
//             rx.into_future().wait().unwrap().0.unwrap(),
//             Message::Event("/a/b".into(), "moo".into())
//         );
//     }
//
//     #[test]
//     fn test_route() {
//         let (tx, rx) = mpsc::channel(10);
//
//         let mut receivers = HashMap::new();
//         receivers.insert("/a/b".into(), vec![tx]);
//         let receivers = Mutex::new(receivers);
//         let receivers_c = receivers.clone();
//
//         let event_msg = Message::Event("/a/b".into(), "Moo!".into());
//         let event_msg_c = event_msg.clone();
//
//         // All this extra fluff around future::lazy() is necessary to ensure that there
//         // is an active executor when the fn calls Mutex::with().
//         tokio::run(future::lazy(move || {
//             route(&receivers, event_msg).map_err(|_| ())
//         }));
//
//         assert_eq!(rx.into_future().wait().unwrap().0.unwrap(), event_msg_c);
//         assert!(receivers_c
//             .try_unwrap()
//             .unwrap()
//             .contains_key(&"/a/b".into()));
//     }
//
//     #[test]
//     fn test_route_norecv() {
//         let (tx, _) = mpsc::channel(10);
//
//         let mut receivers = HashMap::new();
//         receivers.insert("/a/b".into(), vec![tx]);
//         let receivers = Mutex::new(receivers);
//         let receivers_c = receivers.clone();
//
//         // All this extra fluff around future::lazy() is necessary to ensure that there
//         // is an active executor when the fn calls Mutex::with().
//         tokio::run(future::lazy(move || {
//             route(&receivers, Message::Event("/a/b".into(), "Moo!".into())).map_err(|_| ())
//         }));
//
//         assert!(receivers_c.try_unwrap().unwrap().is_empty());
//     }
// }
