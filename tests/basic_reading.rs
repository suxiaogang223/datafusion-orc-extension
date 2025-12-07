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

//! Integration tests for basic ORC file reading functionality

use datafusion::prelude::*;
use datafusion_datasource::file_format::FileFormat;
use datafusion_orc_extension::OrcFormat;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::path::PathBuf;
use std::sync::Arc;

/// Helper function to create a test ObjectStore
fn create_test_object_store() -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

/// Helper function to get test data directory
fn get_test_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("integration")
        .join("data")
}

#[tokio::test]
async fn test_schema_inference_alltypes() {
    let test_data_dir = get_test_data_dir();
    let orc_file = test_data_dir.join("alltypes.snappy.orc");

    let object_store = create_test_object_store();
    let format = OrcFormat::new();

    // Get file metadata - convert PathBuf to string first
    let file_path: object_store::path::Path = orc_file.to_string_lossy().as_ref().into();
    let file_meta = object_store
        .head(&file_path)
        .await
        .expect("Failed to get file metadata");

    // Create SessionContext and get SessionState
    let ctx = SessionContext::new();
    let session_state = ctx.state();

    // Infer schema
    let schema = format
        .infer_schema(&session_state, &object_store, &[file_meta])
        .await
        .expect("Failed to infer schema");

    // Verify schema is not empty
    assert!(!schema.fields().is_empty(), "Expected non-empty schema");

    println!("Inferred schema with {} fields:", schema.fields().len());
    for field in schema.fields() {
        println!("  - {}: {:?}", field.name(), field.data_type());
    }
}

#[tokio::test]
async fn test_schema_inference_map_list() {
    let test_data_dir = get_test_data_dir();
    let orc_file = test_data_dir.join("map_list.snappy.orc");

    let object_store = create_test_object_store();
    let format = OrcFormat::new();

    // Get file metadata - convert PathBuf to string first
    let file_path: object_store::path::Path = orc_file.to_string_lossy().as_ref().into();
    let file_meta = object_store
        .head(&file_path)
        .await
        .expect("Failed to get file metadata");

    // Create SessionContext and get SessionState
    let ctx = SessionContext::new();
    let session_state = ctx.state();

    // Infer schema
    let schema = format
        .infer_schema(&session_state, &object_store, &[file_meta])
        .await
        .expect("Failed to infer schema");

    // Verify schema is not empty
    assert!(!schema.fields().is_empty(), "Expected non-empty schema");

    println!("Inferred schema with {} fields:", schema.fields().len());
    for field in schema.fields() {
        println!("  - {}: {:?}", field.name(), field.data_type());
    }
}
