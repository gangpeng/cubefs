// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Error types mirroring Go/Rust storage error definitions.

const constants = @import("constants.zig");

/// Unified error type for the CubeFS Zig engine.
pub const Error = error{
    ExtentDeleted,
    ParameterMismatch,
    NoAvailableExtent,
    NoBrokenExtent,
    NoSpace,
    ForbiddenDataPartition,
    TryAgain,
    LimitedIo,
    CrcMismatch,
    ExtentNotFound,
    ExtentExists,
    ExtentFull,
    ExtentBroken,
    BrokenDisk,
    ForbidWrite,
    IoError,
    WriteZero,
    SubmissionQueueFull,
    NoCompletion,
    ExtentLocked,
    NotLeader,
    RaftSubmitFailed,
};

/// Convert an error to an FFI-compatible error code.
pub fn toFfiCode(err: Error) i32 {
    return switch (err) {
        error.ParameterMismatch => constants.CUBEFS_ERR_INVALID_ARG,
        error.ExtentNotFound, error.NoAvailableExtent, error.NoBrokenExtent => constants.CUBEFS_ERR_NOT_FOUND,
        error.ExtentExists => constants.CUBEFS_ERR_ALREADY_EXISTS,
        error.NoSpace => constants.CUBEFS_ERR_NO_SPACE,
        error.IoError, error.LimitedIo, error.WriteZero => constants.CUBEFS_ERR_IO,
        error.CrcMismatch => constants.CUBEFS_ERR_CRC_MISMATCH,
        error.ExtentFull => constants.CUBEFS_ERR_EXTENT_FULL,
        error.ExtentDeleted => constants.CUBEFS_ERR_EXTENT_DELETED,
        error.ExtentBroken, error.BrokenDisk => constants.CUBEFS_ERR_EXTENT_BROKEN,
        error.ExtentLocked => constants.CUBEFS_ERR_IO,
        error.NotLeader, error.RaftSubmitFailed => constants.CUBEFS_ERR_INTERNAL,
        else => constants.CUBEFS_ERR_INTERNAL,
    };
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = @import("std").testing;

test "toFfiCode maps ParameterMismatch to INVALID_ARG" {
    try testing.expectEqual(constants.CUBEFS_ERR_INVALID_ARG, toFfiCode(error.ParameterMismatch));
}

test "toFfiCode maps not-found errors to NOT_FOUND" {
    try testing.expectEqual(constants.CUBEFS_ERR_NOT_FOUND, toFfiCode(error.ExtentNotFound));
    try testing.expectEqual(constants.CUBEFS_ERR_NOT_FOUND, toFfiCode(error.NoAvailableExtent));
    try testing.expectEqual(constants.CUBEFS_ERR_NOT_FOUND, toFfiCode(error.NoBrokenExtent));
}

test "toFfiCode maps ExtentExists to ALREADY_EXISTS" {
    try testing.expectEqual(constants.CUBEFS_ERR_ALREADY_EXISTS, toFfiCode(error.ExtentExists));
}

test "toFfiCode maps NoSpace to NO_SPACE" {
    try testing.expectEqual(constants.CUBEFS_ERR_NO_SPACE, toFfiCode(error.NoSpace));
}

test "toFfiCode maps IO errors to IO" {
    try testing.expectEqual(constants.CUBEFS_ERR_IO, toFfiCode(error.IoError));
    try testing.expectEqual(constants.CUBEFS_ERR_IO, toFfiCode(error.LimitedIo));
    try testing.expectEqual(constants.CUBEFS_ERR_IO, toFfiCode(error.WriteZero));
}

test "toFfiCode maps CrcMismatch to CRC_MISMATCH" {
    try testing.expectEqual(constants.CUBEFS_ERR_CRC_MISMATCH, toFfiCode(error.CrcMismatch));
}

test "toFfiCode maps ExtentFull to EXTENT_FULL" {
    try testing.expectEqual(constants.CUBEFS_ERR_EXTENT_FULL, toFfiCode(error.ExtentFull));
}

test "toFfiCode maps ExtentDeleted to EXTENT_DELETED" {
    try testing.expectEqual(constants.CUBEFS_ERR_EXTENT_DELETED, toFfiCode(error.ExtentDeleted));
}

test "toFfiCode maps broken errors to EXTENT_BROKEN" {
    try testing.expectEqual(constants.CUBEFS_ERR_EXTENT_BROKEN, toFfiCode(error.ExtentBroken));
    try testing.expectEqual(constants.CUBEFS_ERR_EXTENT_BROKEN, toFfiCode(error.BrokenDisk));
}

test "toFfiCode maps unknown errors to INTERNAL" {
    try testing.expectEqual(constants.CUBEFS_ERR_INTERNAL, toFfiCode(error.ForbidWrite));
    try testing.expectEqual(constants.CUBEFS_ERR_INTERNAL, toFfiCode(error.TryAgain));
    try testing.expectEqual(constants.CUBEFS_ERR_INTERNAL, toFfiCode(error.SubmissionQueueFull));
    try testing.expectEqual(constants.CUBEFS_ERR_INTERNAL, toFfiCode(error.NoCompletion));
}

test "all Error variants are covered by toFfiCode" {
    // Exhaustive test: every error in the Error set should produce a valid FFI code
    const errors = [_]Error{
        error.ExtentDeleted,       error.ParameterMismatch,    error.NoAvailableExtent,
        error.NoBrokenExtent,      error.NoSpace,              error.ForbiddenDataPartition,
        error.TryAgain,            error.LimitedIo,            error.CrcMismatch,
        error.ExtentNotFound,      error.ExtentExists,         error.ExtentFull,
        error.ExtentBroken,        error.BrokenDisk,           error.ForbidWrite,
        error.IoError,             error.WriteZero,            error.SubmissionQueueFull,
        error.NoCompletion,        error.ExtentLocked,         error.NotLeader,
        error.RaftSubmitFailed,
    };
    for (errors) |e| {
        const code = toFfiCode(e);
        // All codes should be negative (error codes)
        try testing.expect(code < 0);
    }
}
