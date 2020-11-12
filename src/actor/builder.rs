use super::{Actor, AknShutdown, LauncherSender, Passthrough, Shutdown, Starter};
use serde::{Deserialize, Serialize};

pub trait Builder {
    type State;
    fn build(self) -> Self::State;
}

/// Should be implemented on the ActorBuilder struct
pub trait ActorBuilder<H: AknShutdown<Self::State> + 'static>: Builder
where
    Self::State: Actor<H>,
{
}

/// Should be implemented on the AppBuilder struct
pub trait AppBuilder<H: LauncherSender<Self::Through> + AknShutdown<Self::State>>: Builder + ThroughType + Clone + Starter<H>
where
    Self::State: Actor<H>,
    Self::Ok: Shutdown + Passthrough<Self::Through>,
    Self::Input: From<Self::State>,
    Self::Error: Serialize,
{
}

pub trait ThroughType {
    /// identfiy the Through which is the event type with predefind functionality to the outdoor (ie websocket msg)
    type Through: for<'de> Deserialize<'de> + Serialize;
}

#[macro_export]
macro_rules! builder {
    ( #[derive($($der:tt),*)] $struct:ident {$( $field:ident: $type:ty ),*} ) => {
        #[derive($($der,)*Default)]
        pub struct $struct {
            $(
                $field: Option<$type>,
            )*
        }
        impl $struct {
            pub fn new() -> Self {
                Self {
                    $(
                        $field: None,
                    )*
                }
            }

            $(
                pub fn $field(mut self, $field: $type) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
        }

    };
    (#[derive($($der:tt),*)] $struct:ident<$( $extra:tt ),*> {$( $field:ident:$type:tt ),*}) => {
        #[derive($($der,)*Default)]
        pub struct $struct< $( $extra ),* > {
            $(
                $field: Option<$type>,
            )*
            #[allow(unused_parens)]
            _phantom: std::marker::PhantomData<($( $extra ),*)>
        }

        impl<$( $extra ),*> $struct< $( $extra ),* > {
            pub fn new() -> Self {
                Self {
                    $(
                        $field: None,
                    )*
                    _phantom: std::marker::PhantomData,
                }
            }
            $(
                pub fn $field(mut self, $field: $type) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
        }
    };

    ($struct:ident {$( $field:ident: $type:ty ),*}) => {
        #[derive(Default)]
        pub struct $struct {
            $(
                $field: Option<$type>,
            )*
        }
        impl $struct {
            pub fn new() -> Self {
                Self {
                    $(
                        $field: None,
                    )*
                }
            }

            $(
                pub fn $field(mut self, $field: $type) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
        }
    };
    ($struct:ident<$( $extra:tt ),*> {$( $field:ident:$type:tt ),*}) => {
        #[derive(Default)]
        pub struct $struct< $( $extra ),* > {
            $(
                $field: Option<$type>,
            )*
            #[allow(unused_parens)]
            _phantom: std::marker::PhantomData<($( $extra ),*)>
        }

        impl<$( $extra ),*> $struct< $( $extra ),* > {
            pub fn new() -> Self {
                Self {
                    $(
                        $field: None,
                    )*
                    _phantom: std::marker::PhantomData,
                }
            }
            $(
                pub fn $field(mut self, $field: $type) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
        }
    };

}
