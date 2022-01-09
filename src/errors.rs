use cord_message::{errors::Error as MessageError, Message};
use error_chain::*;
use std::io::Error as IoError;
use tokio::sync::{
    mpsc::error::{SendError, TrySendError},
    oneshot::error::RecvError as OneshotRecvError,
};

error_chain! {
    foreign_links {
        ClientSend(TrySendError<Message>);
        ClientForward(SendError<Message>);
        Io(IoError);
        Message(MessageError);
        Terminate(OneshotRecvError);
    }
}
