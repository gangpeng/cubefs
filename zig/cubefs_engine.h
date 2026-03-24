#ifndef CUBEFS_ENGINE_H
#define CUBEFS_ENGINE_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct ExtentStore ExtentStore;
typedef struct EcEncoder EcEncoder;
typedef struct Arc_BufferPool Arc_BufferPool;

// C-compatible extent info struct for FFI.
typedef struct CExtentInfo {
  uint64_t file_id;
  uint64_t size;
  uint32_t crc;
  bool is_deleted;
  int64_t modify_time;
  uint64_t snapshot_data_off;
  uint64_t apply_id;
} CExtentInfo;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

// Create a new extent store at the given directory path.
//
// Returns an opaque handle, or null on failure.
//
// # Safety
// `data_path` must be a valid null-terminated C string.
ExtentStore *cubefs_extent_store_new(const char *data_path, uint64_t partition_id);

// Destroy an extent store, flushing and closing all extents.
//
// # Safety
// `store` must be a valid pointer returned by `cubefs_extent_store_new`.
void cubefs_extent_store_free(ExtentStore *store);

// Create a new extent in the store.
//
// # Safety
// `store` must be a valid pointer.
int32_t cubefs_extent_create(ExtentStore *store, uint64_t extent_id);

// Allocate the next extent ID.
//
// # Safety
// `store` must be a valid pointer.
uint64_t cubefs_extent_next_id(ExtentStore *store);

// Write data to an extent.
//
// # Safety
// `store` must be a valid pointer. `data` must point to `data_len` readable bytes.
int32_t cubefs_extent_write(ExtentStore *store,
                            uint64_t extent_id,
                            int64_t offset,
                            const uint8_t *data,
                            uintptr_t data_len,
                            uint32_t crc,
                            int32_t write_type,
                            bool is_sync);

// Read data from an extent.
//
// On success, writes the CRC to `crc_out`, bytes read to `bytes_read`, and returns 0.
// On failure, returns a negative error code.
//
// # Safety
// `store` must be a valid pointer. `buf` must point to `buf_len` writable bytes.
// `bytes_read` and `crc_out` must be valid pointers.
int32_t cubefs_extent_read(ExtentStore *store,
                           uint64_t extent_id,
                           uint64_t offset,
                           uint8_t *buf,
                           uintptr_t buf_len,
                           uintptr_t *bytes_read,
                           uint32_t *crc_out);

// Mark an extent as deleted.
//
// # Safety
// `store` must be a valid pointer.
int32_t cubefs_extent_delete(ExtentStore *store, uint64_t extent_id);

// Get extent info by ID.
//
// # Safety
// `store` must be a valid pointer. `info_out` must be a valid pointer.
int32_t cubefs_extent_info(ExtentStore *store, uint64_t extent_id, struct CExtentInfo *info_out);

// Get the number of extents in the store.
//
// # Safety
// `store` must be a valid pointer.
uintptr_t cubefs_extent_count(ExtentStore *store);

// Get a snapshot of all extent info.
//
// Allocates an array of `CExtentInfo` and writes its address to `out_ptr`,
// count to `out_len`. Caller must free with `cubefs_extent_infos_free`.
//
// # Safety
// `store`, `out_ptr`, and `out_len` must be valid pointers.
int32_t cubefs_extent_store_snapshot(ExtentStore *store,
                                     struct CExtentInfo **out_ptr,
                                     uintptr_t *out_len);

// Get watermarks (non-deleted extent info).
//
// Same semantics as `cubefs_extent_store_snapshot` but filters out deleted extents.
//
// # Safety
// `store`, `out_ptr`, and `out_len` must be valid pointers.
int32_t cubefs_extent_store_watermarks(ExtentStore *store,
                                       struct CExtentInfo **out_ptr,
                                       uintptr_t *out_len);

// Free an array of CExtentInfo returned by snapshot/watermarks.
//
// # Safety
// `ptr` must have been returned by `cubefs_extent_store_snapshot` or
// `cubefs_extent_store_watermarks`, and `len` must match.
void cubefs_extent_infos_free(struct CExtentInfo *ptr, uintptr_t len);

// Check if the store has an extent with the given ID.
//
// # Safety
// `store` must be a valid pointer.
bool cubefs_extent_store_has_extent(ExtentStore *store, uint64_t extent_id);

// Get total used size across all non-deleted extents.
//
// # Safety
// `store` must be a valid pointer.
uint64_t cubefs_extent_store_used_size(ExtentStore *store);

// Flush all cached extents to disk.
//
// # Safety
// `store` must be a valid pointer.
int32_t cubefs_extent_store_flush(ExtentStore *store);

// Get an available tiny extent ID from the pool.
//
// Returns 0 if no tiny extent is available.
//
// # Safety
// `store` must be a valid pointer.
uint64_t cubefs_extent_store_get_tiny(ExtentStore *store);

// Return a tiny extent ID to the available pool.
//
// # Safety
// `store` must be a valid pointer.
bool cubefs_extent_store_put_tiny(ExtentStore *store, uint64_t extent_id);

// Punch a hole in a tiny extent (range deletion).
//
// # Safety
// `store` must be a valid pointer.
int32_t cubefs_extent_mark_delete_range(ExtentStore *store,
                                        uint64_t extent_id,
                                        int64_t offset,
                                        int64_t size);

// Create a new EC encoder.
//
// # Safety
// Returns an opaque handle, or null on failure.
EcEncoder *cubefs_ec_encoder_new(uintptr_t data_shards, uintptr_t parity_shards);

// Destroy an EC encoder.
//
// # Safety
// `encoder` must be a valid pointer returned by `cubefs_ec_encoder_new`.
void cubefs_ec_encoder_free(EcEncoder *encoder);

// Create a new buffer pool.
struct Arc_BufferPool *cubefs_buffer_pool_new(void);

// Destroy a buffer pool.
//
// # Safety
// `pool` must be a valid pointer returned by `cubefs_buffer_pool_new`.
void cubefs_buffer_pool_free(struct Arc_BufferPool *pool);

// Get the engine version string.
//
// Returns a static string, valid for the lifetime of the library.
const char *cubefs_engine_version(void);

// Initialize the engine.
//
// Call once at program startup.
void cubefs_engine_init(void);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* CUBEFS_ENGINE_H */
