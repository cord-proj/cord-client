use cord_message::Message;
use error_chain::*;
use tokio::sync::mpsc::error::{RecvError, SendError, TrySendError};

error_chain! {
    foreign_links {
        ConnRecv(RecvError);
        ConnSend(TrySendError<Message>);
        ConnForward(SendError<Message>);
        Io(::std::io::Error);
        Message(cord_message::errors::Error);
        Terminate(::tokio::sync::oneshot::error::RecvError);
    }
}
