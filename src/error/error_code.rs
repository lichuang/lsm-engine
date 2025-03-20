// Copyright (c) 2025 Lichuang(codedump)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(non_snake_case)]

use super::backtrace::capture;
use crate::error::Error;

macro_rules! build_error {
    ($($(#[$meta:meta])* $body:ident($code:expr)),*$(,)*) => {
        impl Error {
            $(
                paste::item! {
                    $(
                        #[$meta]
                    )*
                    pub const [< $body:snake:upper >]: u32 = $code;
                }
                $(
                    #[$meta]
                )*
                pub fn $body(display_text: impl Into<String>) -> Error {
                    Error::create(
                        $code,
                        stringify!($body),
                        display_text.into(),
                        String::new(),
                        None,
                        capture(),
                    )
                }
            )*
        }
    }
}

build_error! {
  Ok(0),

  Internal(10),
}
