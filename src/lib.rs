pub mod errors;

use cord_message::{Codec, Message, Pattern};
use errors::{Error, ErrorKind, Result};
use futures::{Future, Sink, StartSend, Stream};
use futures_locks::Mutex;
use retain_mut::RetainMut;
use tokio::{codec::Framed, net::TcpStream, prelude::Async, sync::mpsc, sync::oneshot};

use std::{collections::HashMap, net::SocketAddr, ops::Drop, result, sync::Arc};

/// A `Conn` is used to connect to and communicate with a broker.
///
/// # Examples
///
/// ```
/// use cord_client::Conn;
/// use futures::{future, Future};
/// use tokio;
///
/// let fut = Conn::new("127.0.0.1:7101".parse().unwrap()).and_then(|mut conn| {
///     // Tell the broker we're going to provide the namespace /users
///     conn.provide("/users".into()).unwrap();
///
///     // Start publishing events...
///     conn.event("/users/mark".into(), "Mark has joined").unwrap();
///
///     Ok(())
/// }).map_err(|_| ());
///
/// tokio::run(fut);
/// ```
#[derive(Clone)]
pub struct Conn {
    sender: mpsc::UnboundedSender<Message>,
    inner: Arc<Inner>,
}

/// A `Subscriber` encapsulates a stream of events for a subscribed namespace. It is
/// created by [`Conn::subscribe()`](struct.Conn.html#method.subscribe).
///
/// # Examples
///
/// ```
///# use cord_client::Conn;
///# use cord_client::errors::ErrorKind;
///# use futures::{future, Future, Stream};
///# use tokio;
///
///# let fut = Conn::new("127.0.0.1:7101".parse().unwrap()).and_then(|mut conn| {
/// conn.subscribe("/users/".into()).unwrap().for_each(|(namespace, data)| {
///     // Handle the message...
///     dbg!("Received the namespace '{}' with data: {}", namespace, data);
///
///     Ok(())
/// })
///# });
/// ```
pub struct Subscriber {
    receiver: mpsc::Receiver<Message>,
    _inner: Arc<Inner>,
}

struct Inner {
    receivers: Mutex<HashMap<Pattern, Vec<mpsc::Sender<Message>>>>,
    detonator: Option<oneshot::Sender<()>>,
}

impl Conn {
    /// Connect to a broker
    pub fn new(addr: SocketAddr) -> impl Future<Item = Conn, Error = Error> {
        // Because Sink::send takes ownership of the sink and returns a future, we need a
        // different type to send data that doesn't require ownership. Channels are great
        // for this.
        let (tx, rx) = mpsc::unbounded_channel();

        // This channel is used to shutdown the stream listener when the Conn is dropped
        let (det_tx, det_rx) = oneshot::channel();

        TcpStream::connect(&addr)
            .map(|sock| {
                // Wrap socket in message codec
                let framed = Framed::new(sock, Codec::default());
                let (sink, stream) = framed.split();

                // Drain the channel's receiver into the codec's sink
                tokio::spawn(
                    rx.map_err(|e| Error::from_kind(ErrorKind::ConnRecv(e)))
                        .forward(sink)
                        .map(|_| ())
                        .map_err(|_| ()),
                );

                // Setup the receivers map
                let receivers = Mutex::new(HashMap::new());
                let receivers_c = receivers.clone();

                // Route the codec's stream to receivers
                tokio::spawn(
                    stream
                        .map_err(|e| Error::from_kind(ErrorKind::Message(e)))
                        .for_each(move |message| route(&receivers_c, message))
                        .select(det_rx.map_err(|e| Error::from_kind(ErrorKind::Terminate(e))))
                        .map(|_| ())
                        .map_err(|_| ()),
                );

                Conn {
                    sender: tx,
                    inner: Arc::new(Inner {
                        receivers,
                        detonator: Some(det_tx),
                    }),
                }
            })
            .map_err(|e| ErrorKind::Io(e).into())
    }

    /// Inform the broker that you will be providing a new namespace
    pub fn provide(&mut self, namespace: Pattern) -> Result<()> {
        Ok(self.sender.try_send(Message::Provide(namespace))?)
    }

    /// Inform the broker that you will no longer be providing a namespace
    pub fn revoke(&mut self, namespace: Pattern) -> Result<()> {
        Ok(self.sender.try_send(Message::Revoke(namespace))?)
    }

    /// Subscribe to another provider's namespace
    ///
    /// # Examples
    ///
    /// ```
    ///# use cord_client::Conn;
    ///# use cord_client::errors::ErrorKind;
    ///# use futures::{future, Future, Stream};
    ///# use tokio;
    ///
    ///# let fut = Conn::new("127.0.0.1:7101".parse().unwrap()).and_then(|mut conn| {
    /// conn.subscribe("/users/".into()).unwrap().for_each(|msg| {
    ///     // Handle the message...
    ///     dbg!("The following user just joined: {}", msg);
    ///
    ///     Ok(())
    /// })
    ///# });
    /// ```
    pub fn subscribe(&mut self, namespace: Pattern) -> Result<Subscriber> {
        let namespace_c = namespace.clone();
        self.sender.try_send(Message::Subscribe(namespace))?;

        let (tx, rx) = mpsc::channel(10);
        tokio::spawn(
            self.inner
                .receivers
                .with(move |mut guard| {
                    (*guard)
                        .entry(namespace_c)
                        .or_insert_with(Vec::new)
                        .push(tx);
                    Ok(())
                })
                .expect("The default executor has shut down")
                .map(|_| ()),
        );
        Ok(Subscriber {
            receiver: rx,
            _inner: self.inner.clone(),
        })
    }

    /// Unsubscribe from another provider's namespace
    pub fn unsubscribe(&mut self, namespace: Pattern) -> Result<()> {
        let namespace_c = namespace.clone();
        self.sender.try_send(Message::Unsubscribe(namespace))?;

        tokio::spawn(
            self.inner
                .receivers
                .with(move |mut guard| {
                    (*guard).remove(&namespace_c);
                    Ok(())
                })
                .expect("The default executor has shut down")
                .map(|_| ()),
        );
        Ok(())
    }

    /// Publish an event to your subscribers
    pub fn event<S: Into<String>>(&mut self, namespace: Pattern, data: S) -> Result<()> {
        Ok(self
            .sender
            .try_send(Message::Event(namespace, data.into()))?)
    }
}

impl Sink for Conn {
    type SinkItem = Message;
    type SinkError = mpsc::error::UnboundedSendError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.sender.start_send(item)
    }

    fn poll_complete(&mut self) -> result::Result<Async<()>, Self::SinkError> {
        self.sender.poll_complete()
    }
}

fn route(
    receivers: &Mutex<HashMap<Pattern, Vec<mpsc::Sender<Message>>>>,
    message: Message,
) -> impl Future<Item = (), Error = Error> {
    receivers
        .with(move |mut guard| {
            // Remove any subscribers that have no senders left
            (*guard).retain(|namespace, senders| {
                // We assume that all messages will be Events. If this changes, we will
                // need to store a Message, not a pattern.
                if namespace.contains(message.namespace()) {
                    // Remove any senders that give errors when attempting to send
                    senders.retain_mut(|tx| tx.try_send(message.clone()).is_ok());

                    // XXX Awaiting stabilisation
                    // https://github.com/rust-lang/rust/issues/43244
                    // senders.drain_filter(|tx| {
                    //     tx.try_send(message.clone()).is_ok()
                    // });
                }

                // So long as we have senders, keep the subscriber
                !senders.is_empty()
            });

            Ok(())
        })
        .expect("The default executor has shut down")
}

impl Stream for Subscriber {
    type Item = (Pattern, String);
    type Error = Error;

    fn poll(&mut self) -> result::Result<Async<Option<Self::Item>>, Self::Error> {
        self.receiver
            .poll()
            .map(|asy| {
                asy.map(|opt| {
                    opt.map(|msg| match msg {
                        Message::Event(pattern, data) => (pattern, data),
                        _ => unreachable!(),
                    })
                })
            })
            .map_err(|e| ErrorKind::SubscriberError(e).into())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.detonator.take().unwrap().send(()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;
    use tokio::prelude::Async;

    struct ForwardStream(Vec<Message>);
    impl Stream for ForwardStream {
        type Item = Message;
        type Error = Error;

        fn poll(&mut self) -> Result<Async<Option<Self::Item>>> {
            Ok(Async::Ready(self.0.pop()))
        }
    }

    #[test]
    fn test_forward() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (det_tx, _det_rx) = oneshot::channel();

        let conn = Conn {
            sender: tx,
            inner: Arc::new(Inner {
                receivers: Mutex::new(HashMap::new()),
                detonator: Some(det_tx),
            }),
        };

        let data_stream = ForwardStream(vec![
            Message::Event("/a".into(), "b".into()),
            Message::Provide("/a".into()),
        ]);
        data_stream.forward(conn).wait().unwrap();

        // We check these messages in reverse order (i.e. Provide, then Event), because
        // our budget DIY stream sends them in reverse order.
        let (item, rx) = rx.into_future().wait().unwrap();
        assert_eq!(item, Some(Message::Provide("/a".into())));

        let (item, _) = rx.into_future().wait().unwrap();
        assert_eq!(item, Some(Message::Event("/a".into(), "b".into())));
    }

    #[test]
    fn test_provide() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (det_tx, _det_rx) = oneshot::channel();

        let mut conn = Conn {
            sender: tx,
            inner: Arc::new(Inner {
                receivers: Mutex::new(HashMap::new()),
                detonator: Some(det_tx),
            }),
        };

        conn.provide("/a/b".into()).unwrap();
        assert_eq!(
            rx.into_future().wait().unwrap().0.unwrap(),
            Message::Provide("/a/b".into())
        );
    }

    #[test]
    fn test_revoke() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (det_tx, _det_rx) = oneshot::channel();

        let mut conn = Conn {
            sender: tx,
            inner: Arc::new(Inner {
                receivers: Mutex::new(HashMap::new()),
                detonator: Some(det_tx),
            }),
        };

        conn.revoke("/a/b".into()).unwrap();
        assert_eq!(
            rx.into_future().wait().unwrap().0.unwrap(),
            Message::Revoke("/a/b".into())
        );
    }

    #[test]
    fn test_subscribe() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (det_tx, _det_rx) = oneshot::channel();

        let receivers: Mutex<HashMap<Pattern, Vec<mpsc::Sender<Message>>>> =
            Mutex::new(HashMap::new());

        let mut conn = Conn {
            sender: tx,
            inner: Arc::new(Inner {
                receivers: receivers.clone(),
                detonator: Some(det_tx),
            }),
        };

        // All this extra fluff around future::lazy() is necessary to ensure that there
        // is an active executor when the fn calls Mutex::with().
        tokio::run(future::lazy(move || {
            conn.subscribe("/a/b".into()).unwrap();
            Ok(())
        }));
        assert_eq!(
            rx.into_future().wait().unwrap().0.unwrap(),
            Message::Subscribe("/a/b".into())
        );
        assert!(receivers.try_unwrap().unwrap().contains_key(&"/a/b".into()));
    }

    #[test]
    fn test_unsubscribe() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (det_tx, _det_rx) = oneshot::channel();

        let mut receivers: HashMap<Pattern, Vec<mpsc::Sender<Message>>> = HashMap::new();
        receivers.insert("/a/b".into(), Vec::new());
        let receivers = Mutex::new(receivers);

        let mut conn = Conn {
            sender: tx,
            inner: Arc::new(Inner {
                receivers: receivers.clone(),
                detonator: Some(det_tx),
            }),
        };

        // All this extra fluff around future::lazy() is necessary to ensure that there
        // is an active executor when the fn calls Mutex::with().
        tokio::run(future::lazy(move || {
            conn.unsubscribe("/a/b".into()).unwrap();
            Ok(())
        }));
        assert_eq!(
            rx.into_future().wait().unwrap().0.unwrap(),
            Message::Unsubscribe("/a/b".into())
        );
        assert!(receivers.try_unwrap().unwrap().is_empty());
    }

    #[test]
    fn test_event() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (det_tx, _det_rx) = oneshot::channel();

        let mut conn = Conn {
            sender: tx,
            inner: Arc::new(Inner {
                receivers: Mutex::new(HashMap::new()),
                detonator: Some(det_tx),
            }),
        };

        conn.event("/a/b".into(), "moo").unwrap();
        assert_eq!(
            rx.into_future().wait().unwrap().0.unwrap(),
            Message::Event("/a/b".into(), "moo".into())
        );
    }

    #[test]
    fn test_route() {
        let (tx, rx) = mpsc::channel(10);

        let mut receivers = HashMap::new();
        receivers.insert("/a/b".into(), vec![tx]);
        let receivers = Mutex::new(receivers);
        let receivers_c = receivers.clone();

        let event_msg = Message::Event("/a/b".into(), "Moo!".into());
        let event_msg_c = event_msg.clone();

        // All this extra fluff around future::lazy() is necessary to ensure that there
        // is an active executor when the fn calls Mutex::with().
        tokio::run(future::lazy(move || {
            route(&receivers, event_msg).map_err(|_| ())
        }));

        assert_eq!(rx.into_future().wait().unwrap().0.unwrap(), event_msg_c);
        assert!(receivers_c
            .try_unwrap()
            .unwrap()
            .contains_key(&"/a/b".into()));
    }

    #[test]
    fn test_route_norecv() {
        let (tx, _) = mpsc::channel(10);

        let mut receivers = HashMap::new();
        receivers.insert("/a/b".into(), vec![tx]);
        let receivers = Mutex::new(receivers);
        let receivers_c = receivers.clone();

        // All this extra fluff around future::lazy() is necessary to ensure that there
        // is an active executor when the fn calls Mutex::with().
        tokio::run(future::lazy(move || {
            route(&receivers, Message::Event("/a/b".into(), "Moo!".into())).map_err(|_| ())
        }));

        assert!(receivers_c.try_unwrap().unwrap().is_empty());
    }
}
