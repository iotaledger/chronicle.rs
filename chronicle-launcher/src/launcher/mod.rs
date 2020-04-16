#[macro_export]
macro_rules! launcher {
    ($name:ident {$($app:ident : $t:ty),+}) => {
        use tokio::sync::mpsc;
        pub type Sender = mpsc::UnboundedSender<String>;
        pub type Receiver = mpsc::UnboundedReceiver<String>;
        #[derive(Default)]
        pub struct $name {
            tx: Option<Sender>,
            rx: Option<Receiver>,
            $(
                $app: Option<$t>,
            )*
        }
        pub struct Apps {
            app_count: usize,
            tx: Sender,
            rx: Receiver,
            $(
                $app: Option<$t>,
            )*
        }
        impl Apps {
            $(
                async fn $app(mut self) -> Self {
                    self.$app.take().unwrap().build().run().await;
                    self
                }
            )*
            // this will break once all apps send break events
            async fn all(mut self) {
                while let Some(_) = self.rx.recv().await {
                    self.app_count -= 1;
                    if self.app_count == 0 {
                        break
                    }
                }
            }
            // this will break once any app send break event
            async fn one(mut self) {
                while let Some(_) = self.rx.recv().await {
                    break
                }
            }
        }
        impl $name {
            pub fn new() -> Self {
                let (tx, rx) = mpsc::unbounded_channel::<String>();
                let mut launcher = Self::default();
                launcher.tx.replace(tx);
                launcher.rx.replace(rx);
                launcher
            }

            pub fn clone_tx(&self) -> Sender {
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
