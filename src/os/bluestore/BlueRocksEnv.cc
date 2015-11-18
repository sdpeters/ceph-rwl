// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BlueRocksEnv.h"
#include "BlueFS.h"

// A file abstraction for reading sequentially through a file
class BlueRocksSequentialFile {
 public:
  BlueRocksSequentialFile() { }

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  Status Read(size_t n, Slice* result, char* scratch);

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  Status Skip(uint64_t n);

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  Status InvalidateCache(size_t offset, size_t length) {
    return Status::NotSupported("InvalidateCache not supported.");
  }
};

// A file abstraction for randomly reading the contents of a file.
class BlueRocksRandomAccessFile {
 public:
  BlueRocksRandomAccessFile() { }

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // Safe for concurrent use by multiple threads.
  Status Read(uint64_t offset, size_t n, Slice* result,
	      char* scratch) const;

  // Used by the file_reader_writer to decide if the ReadAhead wrapper
  // should simply forward the call and do not enact buffering or locking.
  bool ShouldForwardRawRequest() const {
    return false;
  }

  // For cases when read-ahead is implemented in the platform dependent
  // layer
  void EnableReadAhead() {}

  // Tries to get an unique ID for this file that will be the same each time
  // the file is opened (and will stay the same while the file is open).
  // Furthermore, it tries to make this ID at most "max_size" bytes. If such an
  // ID can be created this function returns the length of the ID and places it
  // in "id"; otherwise, this function returns 0, in which case "id"
  // may not have been modified.
  //
  // This function guarantees, for IDs from a given environment, two unique ids
  // cannot be made equal to eachother by adding arbitrary bytes to one of
  // them. That is, no unique ID is the prefix of another.
  //
  // This function guarantees that the returned ID will not be interpretable as
  // a single varint.
  //
  // Note: these IDs are only valid for the duration of the process.
  size_t GetUniqueId(char* id, size_t max_size) const {
    return 0; // Default implementation to prevent issues with backwards
              // compatibility.
  };

  enum AccessPattern { NORMAL, RANDOM, SEQUENTIAL, WILLNEED, DONTNEED };

  void Hint(AccessPattern pattern) {}

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  Status InvalidateCache(size_t offset, size_t length) {
    return Status::NotSupported("InvalidateCache not supported.");
  }
};


// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class BlueRocksWritableFile {
 public:
  BlueRocksWritableFile()
    : last_preallocated_block_(0),
      preallocation_block_size_(0),
      io_priority_(Env::IO_TOTAL) {
  }

  // Indicates if the class makes use of unbuffered I/O
  bool UseOSBuffer() const {
    return true;
  }

  const size_t c_DefaultPageSize = 4 * 1024;

  // This is needed when you want to allocate
  // AlignedBuffer for use with file I/O classes
  // Used for unbuffered file I/O when UseOSBuffer() returns false
  size_t GetRequiredBufferAlignment() const {
    return c_DefaultPageSize;
  }

  Status Append(const Slice& data) = 0;

  // Positioned write for unbuffered access default forward
  // to simple append as most of the tests are buffered by default
  Status PositionedAppend(const Slice& /* data */, uint64_t /* offset */) {
    return Status::NotSupported();
  }

  // Truncate is necessary to trim the file to the correct size
  // before closing. It is not always possible to keep track of the file
  // size due to whole pages writes. The behavior is undefined if called
  // with other writes to follow.
  Status Truncate(uint64_t size);
  Status Close();
  Status Flush();
  Status Sync(); // sync data

  // true if Sync() and Fsync() are safe to call concurrently with Append()
  // and Flush().
  bool IsSyncThreadSafe() const {
    return true;
  }

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  bool UseDirectIO() const { return true; }

  /*
   * Get the size of valid data in the file.
   */
  uint64_t GetFileSize();

  // For documentation, refer to RandomAccessFile::GetUniqueId()
  size_t GetUniqueId(char* id, size_t max_size) const;

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  // This call has no effect on dirty pages in the cache.
  Status InvalidateCache(size_t offset, size_t length) {
    return Status::NotSupported("InvalidateCache not supported.");
  }

  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  // Default implementation does nothing.
  Status RangeSync(off_t offset, off_t nbytes) { return Status::OK(); }

 protected:
  /*
   * Pre-allocate space for a file.
   */
  virtual Status Allocate(off_t offset, off_t len);
};


// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class BlueRocksDirectory {
 public:
  // Fsync directory. Can be called concurrently from multiple threads.
  Status Fsync();
};


// --------------------
// --- BlueRocksEnv ---
// --------------------

BlueRocksEnv::BlueRocksEnv(BlueFS *f)
  : fs(f)
{
  
}

