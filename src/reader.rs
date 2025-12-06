// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! ORC file reading logic

use bytes::Bytes;
use futures_util::future::BoxFuture;
use object_store::path::Path;
use object_store::ObjectStore;
use orc_rust::reader::AsyncChunkReader;
use std::sync::Arc;

/// Adapter to convert ObjectStore to AsyncChunkReader for orc-rust
pub struct ObjectStoreChunkReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: Option<u64>,
}

impl ObjectStoreChunkReader {
    /// Create a new ObjectStoreChunkReader
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            file_size: None,
        }
    }

    /// Create with known file size (for optimization)
    pub fn with_size(store: Arc<dyn ObjectStore>, path: Path, size: u64) -> Self {
        Self {
            store,
            path,
            file_size: Some(size),
        }
    }
}

impl AsyncChunkReader for ObjectStoreChunkReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        Box::pin(async move {
            if let Some(size) = self.file_size {
                Ok(size)
            } else {
                // Fetch metadata to get file size
                let meta =
                    self.store.head(&self.path).await.map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                    })?;
                Ok(meta.size as u64)
            }
        })
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();

        Box::pin(async move {
            let range = offset_from_start..(offset_from_start + length);
            let bytes = store
                .get_range(&path, range)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            Ok(bytes)
        })
    }
}
