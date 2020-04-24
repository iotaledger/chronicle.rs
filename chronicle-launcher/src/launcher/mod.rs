#[macro_export]
macro_rules! launcher {
    (
        apps_builder: $name:ident {$($app:ident : $t:ty),+},
        apps: $apps:ident {$($field:ident : $type:ty),*},
        tx: $tx:ty,
        rx: $rx:ty
    ) => {
        use chronicle_common::traits::{
            launcher::LauncherTx,
            dashboard::DashboardTx,
        };
        #[derive(Default)]
        pub struct $name {
            tx: Option<$tx>,
            rx: Option<$rx>,
            $(
                $app: Option<$t>,
            )*
        }
        pub struct $apps {
            app_count: usize,
            tx: $tx,
            rx: $rx,
            $(
                $app: Option<$t>,
            )*
            $(
                $field: Option<$type>,
            )*
        }
        impl Apps {
            $(
                async fn $app(mut self) -> Self {
                    self.$app.take().unwrap().build().run().await;
                    self
                }
            )*
        }
        impl $name {
            pub fn new(tx: $tx, rx: $rx) -> Self {
                let mut launcher = Self::default();
                launcher.tx.replace(tx);
                launcher.rx.replace(rx);
                launcher
            }

            pub fn clone_tx(&self) -> $tx {
                self.tx.as_ref().unwrap().clone()
            }

            pub fn to_apps(mut self) -> Apps {
                Apps {
                    app_count: self.app_count(),
                    tx: self.tx.take().unwrap(),
                    rx: self.rx.take().unwrap(),
                    $(
                        $app: self.$app,
                    )*
                }
            }
            $(
                pub fn $app(mut self, $app: $t) -> Self {
                    self.$app.replace($app.launcher_tx(self.clone_tx()));
                    self
                }
            )*

            fn app_count(&self) -> usize {
                launcher!(@count $($app),+)
            }
        }

    };
    (@count $t1:tt, $($t:tt),+) => { 1 + launcher!(@count $($t),+) };
    (@count $t:tt) => { 1 };
}
