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

//! This module implements actor macro.

#[macro_export]
/// The actor provides builder-like functionality to reduce codes.
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
