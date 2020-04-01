use cord_message::Message;
use error_chain::*;
use tokio::sync::mpsc::error::{
    RecvError, UnboundedRecvError, UnboundedSendError, UnboundedTrySendError,
};

error_chain! {
    foreign_links {
        ConnRecv(UnboundedRecvError);
        ConnSend(UnboundedTrySendError<Message>);
        ConnForward(UnboundedSendError);
        Io(::std::io::Error);
        Message(cord_message::errors::Error);
        SubscriberError(RecvError);
        Terminate(::tokio::sync::oneshot::error::RecvError);
    }
}
