#[macro_export]
macro_rules! actor {
    ($struct:ident {$( $field:ident:$type:ty ),*}) =>{
        #[derive(Default)]
        /// The field of this actor.
        pub struct $struct {
            $(
                $field: Option<$type>,
            )*
        }
        /// Define actor methods.
        impl $struct {
            /// Create a default actor.
            pub fn new() -> Self {
                Self::default()
            }

            $(
                /// Get the new field values.
                pub fn $field(mut self, $field: $type) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
        }
    };
}
