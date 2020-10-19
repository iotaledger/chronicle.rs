// TODO compute token to enable shard_awareness.
// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! This module implements application macro.

#[macro_export]
/// The app macro provides builder-like functionality and has a transmission channel from launcher.
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
