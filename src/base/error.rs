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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {context}: {error}")]
    IOError {
        context: String,
        error: std::io::Error,
    },

    #[error("Filter error: {context}: {error}")]
    FilterError {
        context: String,
        error: tinysearch_cuckoofilter::CuckooError,
    },

    #[error("bincode ser/der error: {context}: {error}")]
    SerDerError {
        context: String,
        error: bincode::Error,
    },

    #[error("block meta error: {context}")]
    BlockError { context: String },
}

impl Error {
    pub fn io_error(context: impl Into<String>) -> impl FnOnce(std::io::Error) -> Self {
        move |error| Self::IOError {
            context: context.into(),
            error,
        }
    }

    pub fn filter_error(
        context: impl Into<String>,
    ) -> impl FnOnce(tinysearch_cuckoofilter::CuckooError) -> Self {
        move |error| Self::FilterError {
            context: context.into(),
            error,
        }
    }

    pub fn serder_error(context: impl Into<String>) -> impl FnOnce(bincode::Error) -> Self {
        move |error| Self::SerDerError {
            context: context.into(),
            error,
        }
    }

    pub fn block_meta_error(context: impl Into<String>) -> Self {
        Self::BlockError {
            context: context.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
