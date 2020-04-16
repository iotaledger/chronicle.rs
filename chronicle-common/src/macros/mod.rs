#[macro_export]
macro_rules! app {
    ($struct:ident {$( $field:ident:$type:ty ),*}) =>{
        #[derive(Default)]
        pub struct $struct {
            launcher_tx: Option<mpsc::UnboundedSender<String>>,
            $(
                $field: Option<$type>,
            )*
        }
        impl $struct {
            pub fn new() -> Self {
                Self::default()
            }
            pub fn launcher_tx(mut self, launcher_tx: mpsc::UnboundedSender<String>) -> Self {
                self.launcher_tx.replace(launcher_tx);
                self
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
