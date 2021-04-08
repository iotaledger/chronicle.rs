use super::{Actor, AknShutdown, LauncherSender, Passthrough, Shutdown, Starter};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Allows an actor to be built by parts
pub trait Builder {
    /// The "state" (actor type) which is built
    type State;
    /// Build the actor
    fn build(self) -> Self::State;
}

/// Should be implemented on the ActorBuilder struct
pub trait ActorBuilder<H: AknShutdown<Self::State> + 'static>: Builder
where
    Self::State: Actor<H>,
{
}

/// Should be implemented on the AppBuilder struct
pub trait AppBuilder<H: LauncherSender<Self> + AknShutdown<Self::State>>: Builder + ThroughType + Clone + Starter<H>
where
    Self::State: Actor<H>,
    Self::Ok: Shutdown + Passthrough<Self::Through>,
    Self::Input: From<Self::State>,
    Self::Error: Display,
{
}

pub trait ThroughType {
    /// identfiy the Through which is the event type with predefind functionality to the outdoor (ie websocket msg)
    type Through: for<'de> Deserialize<'de> + Serialize;
}

/// Create the builder type for an actor, which can be used to construct an actor by parts
#[macro_export]
macro_rules! builder {
    ( $(#[derive($($der:tt),*)])?  $struct:ident {$( $field:ident: $type:tt$(<$($i:tt),*>)? ),*} ) => {
        #[allow(missing_docs)]
        #[derive($($($der,)*)?Default)]
        pub struct $struct {
            $(
                $field: Option<$type$(<$($i,)*>)?>,
            )*
        }

        impl $struct {
            /// Create a new $struct
            pub fn new() -> Self {
                Self {
                    $(
                        $field: None,
                    )*
                }
            }

            $(
                /// Set $field on the builder
                pub fn $field(mut self, $field: $type$(<$($i,)*>)?) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
        }

    };
    ($(#[derive($($der:tt),*)])? $struct:ident$(<$($extra:tt),*>)? {$( $field:ident: $type:tt$(<$($i:tt),*>)? ),*}) => {
        #[derive($($($der,)*)?Default)]
        #[allow(missing_docs)]
        pub struct $struct$(<$($extra,)*>)? {
            $(
                $field: Option<$type$(<$($i,)*>)?>,
            )*
            #[allow(unused_parens)]
            $(
                _phantom: std::marker::PhantomData<$($extra,)*>
            )?
        }

        impl$(<$($extra,)*>)? $struct$(<$($extra,)*>)? {
            /// Create a new $struct
            pub fn new() -> Self {
                Self {
                    $(
                        $field: None,
                    )*
                    _phantom: std::marker::PhantomData,
                }
            }
            $(
                /// Set $field on the builder
                pub fn $field(mut self, $field: $type$(<$($i,)*>)?) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
        }
    };


}
