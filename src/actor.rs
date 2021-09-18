use std::error::Error;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::channel::oneshot;
use futures::future::FutureExt;
use log::error;

use crate::Addr;

/// The type of error returned by an actor method.
pub type ActorError = Box<dyn Error + Send + Sync>;
/// Short alias for a `Result<Produces<T>, ActorError>`.
pub type ActorResult<T, E=ActorError> = Result<Produces<T, E>, E>;

/// An error indicating failure to deliver a method's result to the caller.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Canceled;

impl std::fmt::Display for Canceled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Canceled")
    }
}

impl std::error::Error for Canceled {}

/// A concrete type similar to a `BoxFuture<'static, Result<T, Canceled>>`, but
/// without requiring an allocation if the value is immediately ready.
/// This type implements the `Future` trait and can be directly `await`ed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Produces<T, E=ActorError>
where
    E: From<Canceled>
{
    /// No value was produced.
    None,
    /// A value is ready.
    Value(T),
    /// An error is ready.
    Error(E),
    /// A value may be sent in the future.
    Deferred(oneshot::Receiver<Produces<T, E>>),
}

impl<T, E: From<Canceled>> Unpin for Produces<T, E> {}

impl<T, E: From<Canceled>> Produces<T, E> {
    /// Returns `Ok(Produces::Value(value))`
    pub fn ok(value: T) -> ActorResult<T, E> {
        Ok(Produces::Value(value))
    }

    /// Convert into an error if possible.
    pub fn into_some_error(self) -> Option<E> {
        match self {
            Self::None => Some(Canceled.into()),
            Self::Error(error) => Some(error),
            _ => None,
        }
    }
}

impl<T, E: From<Canceled>> Future for Produces<T, E> {
    type Output = Result<T, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            break match mem::replace(&mut *self, Produces::None) {
                Produces::None => Poll::Ready(Err(Canceled.into())),
                Produces::Value(value) => Poll::Ready(Ok(value)),
                Produces::Error(error) => Poll::Ready(Err(error)),
                Produces::Deferred(mut recv) => match recv.poll_unpin(cx) {
                    Poll::Ready(Ok(producer)) => {
                        *self = producer;
                        continue;
                    }
                    Poll::Ready(Err(_)) => Poll::Ready(Err(Canceled.into())),
                    Poll::Pending => {
                        *self = Produces::Deferred(recv);
                        Poll::Pending
                    }
                },
            };
        }
    }
}

/// Trait implemented by all actors.
/// This trait is defined using the `#[async_trait]` attribute:
/// ```ignore
/// #[async_trait]
/// pub trait Actor: Send + 'static {
///     /// Called automatically when an actor is started. Actors can use this
///     /// to store their own address for future use.
///     async fn started(&mut self, _addr: Addr<Self>) -> ActorResult<()>
///     where
///         Self: Sized,
///     {
///         Ok(())
///     }
///
///     /// Called when any actor method returns an error. If this method
///     /// returns `true`, the actor will stop.
///     /// The default implementation logs the error using the `log` crate
///     /// and then stops the actor.
///     async fn error(&mut self, error: ActorError) -> bool {
///         error!("{}", error);
///         true
///     }
/// }
/// ```
///
/// In order to use a trait object with the actor system, such as with `Addr<dyn Trait>`,
/// the trait must extend this `Actor` trait.
#[async_trait]
pub trait Actor: Send + 'static {
    /// Called automatically when an actor is started. Actors can use this
    /// to store their own address for future use.
    async fn started(&mut self, _addr: Addr<Self>) -> ActorResult<()>
    where
        Self: Sized,
    {
        Produces::ok(())
    }

    /// Called when any actor method returns an error. If this method
    /// returns `true`, the actor will stop.
    /// The default implementation logs the error using the `log` crate
    /// and then stops the actor.
    async fn error(&mut self, error: ActorError) -> bool {
        error!("{}", error);
        true
    }
}

/// Actor methods may return any type implementing this trait.
pub trait IntoActorResult {
    /// The type to be sent back to the caller.
    type Output;
    /// The error type to be sent back to the caller.
    type Error: From<Canceled> + Into<ActorError>;
    /// Perform the conversion to an ActorResult.
    fn into_actor_result(self) -> ActorResult<Self::Output, Self::Error>;
}

impl<T, E> IntoActorResult for ActorResult<T, E>
where
    E: From<Canceled> + Into<ActorError>,
{
    type Output = T;
    type Error = E;
    fn into_actor_result(self) -> ActorResult<T, E> {
        self
    }
}

impl IntoActorResult for () {
    type Output = ();
    type Error = Canceled;
    fn into_actor_result(self) -> ActorResult<(), Self::Error> {
        Produces::ok(())
    }
}
