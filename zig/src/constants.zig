// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Constants mirroring the Go and Rust codebase values.

/// 1 KB
pub const KB: u64 = 1024;

/// 1 MB
pub const MB: u64 = 1024 * KB;

/// 1 GB
pub const GB: u64 = 1024 * MB;

/// 1 TB
pub const TB: u64 = 1024 * GB;

/// Block size for CRC granularity (4 KB).
pub const BLOCK_SIZE: u64 = 4 * KB;

/// Normal extent size boundary (64 MB).
pub const EXTENT_SIZE: u64 = 64 * MB;

/// Maximum extent size (4 TB).
pub const EXTENT_MAX_SIZE: u64 = 4 * TB;

/// Page size for aligned I/O.
pub const PAGE_SIZE: usize = 4096;

/// Number of tiny extent slots (IDs 1..=64).
pub const TINY_EXTENT_COUNT: u64 = 64;

/// First normal extent ID.
pub const MIN_EXTENT_ID: u64 = 1024;

/// Sentinel value indicating an extent has been closed.
pub const EXTENT_HAS_CLOSE: i32 = -1;

/// Write type: append.
pub const WRITE_TYPE_APPEND: i32 = 1;

/// Write type: random write.
pub const WRITE_TYPE_RANDOM: i32 = 2;

/// Write type: append-random (combined).
pub const WRITE_TYPE_APPEND_RANDOM: i32 = 4;

/// FFI result codes.
pub const CUBEFS_OK: i32 = 0;
pub const CUBEFS_ERR_INVALID_ARG: i32 = -1;
pub const CUBEFS_ERR_NOT_FOUND: i32 = -2;
pub const CUBEFS_ERR_ALREADY_EXISTS: i32 = -3;
pub const CUBEFS_ERR_NO_SPACE: i32 = -4;
pub const CUBEFS_ERR_IO: i32 = -5;
pub const CUBEFS_ERR_CRC_MISMATCH: i32 = -6;
pub const CUBEFS_ERR_EXTENT_FULL: i32 = -7;
pub const CUBEFS_ERR_EXTENT_DELETED: i32 = -8;
pub const CUBEFS_ERR_EXTENT_BROKEN: i32 = -9;
pub const CUBEFS_ERR_INTERNAL: i32 = -10;
