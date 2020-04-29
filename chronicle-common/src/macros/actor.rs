#[macro_export]
macro_rules! actor {
    ($struct:ident {$( $field:ident:$type:ty ),*}) =>{
        #[derive(Default)]
        pub struct $struct {
            $(
                $field: Option<$type>,
            )*
        }
        impl $struct {
            pub fn new() -> Self {
                Self::default()
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
