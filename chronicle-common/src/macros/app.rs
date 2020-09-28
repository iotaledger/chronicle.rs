#[macro_export]
macro_rules! app {
    ($struct:ident {$( $field:ident:$type:ty ),*}) =>{
        use chronicle_common::traits::launcher::LauncherTx;
        #[derive(Default,Clone)]
        /// The application has their own fields with the `launcher_tx`, which can send signals
        /// back to the launcher who starts this application.
        pub struct $struct {
            launcher_tx: Option<Box<dyn LauncherTx>>,
            $(
                $field: Option<$type>,
            )*
        }
        impl $struct {
            /// Create a default application.
            pub fn new() -> Self {
                Self::default()
            }
            /// Get the `launcher_tx`.
            pub fn launcher_tx(mut self, launcher_tx: Box<dyn LauncherTx>) -> Self {
                self.launcher_tx.replace(launcher_tx);
                self
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
