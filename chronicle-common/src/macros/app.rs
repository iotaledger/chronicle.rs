#[macro_export]
macro_rules! app {
    ($struct:ident {$( $field:ident:$type:ty ),*}) =>{
        use chronicle_common::traits::launcher::LauncherTx;
        #[derive(Default,Clone)]
        pub struct $struct {
            launcher_tx: Option<Box<dyn LauncherTx>>,
            $(
                $field: Option<$type>,
            )*
        }
        impl $struct {
            pub fn new() -> Self {
                Self::default()
            }
            pub fn launcher_tx(mut self, launcher_tx: Box<dyn LauncherTx>) -> Self {
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
