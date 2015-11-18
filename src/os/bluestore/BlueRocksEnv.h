// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_BLUEROCKSENV_H
#define CEPH_OS_BLUESTORE_BLUEROCKSENV_H

#include "rocksdb/env.h"

class BlueFS;

class BlueRocksEnv : public rocksdb::Env {
  // Return a default environment suitable for the current operating
  // system.  Sophisticated users may wish to provide their own Env
  // implementation instead of relying on this default environment.
  //
  // The result of Default() belongs to rocksdb and must never be deleted.
  static Env* Default();

  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  Status NewSequentialFile(const std::string& fname,
			   unique_ptr<SequentialFile>* result,
			   const EnvOptions& options);

  // Create a brand new random access read-only file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  Status NewRandomAccessFile(const std::string& fname,
			     unique_ptr<RandomAccessFile>* result,
			     const EnvOptions& options);

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  Status NewWritableFile(const std::string& fname,
			 unique_ptr<WritableFile>* result,
			 const EnvOptions& options);

  // Reuse an existing file by renaming it and opening it as writable.
  Status ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   unique_ptr<WritableFile>* result,
                                   const EnvOptions& options);

  // Create an object that represents a directory. Will fail if directory
  // doesn't exist. If the directory exists, it will open the directory
  // and create a new Directory object.
  //
  // On success, stores a pointer to the new Directory in
  // *result and returns OK. On failure stores nullptr in *result and
  // returns non-OK.
  Status NewDirectory(const std::string& name,
                              unique_ptr<Directory>* result);

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  Status FileExists(const std::string& fname);

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result);

  // Delete the named file.
  Status DeleteFile(const std::string& fname);

  // Create the specified directory. Returns error if directory exists.
  Status CreateDir(const std::string& dirname);

  // Creates directory if missing. Return Ok if it exists, or successful in
  // Creating.
  Status CreateDirIfMissing(const std::string& dirname);

  // Delete the specified directory.
  Status DeleteDir(const std::string& dirname);

  // Store the size of fname in *file_size.
  Status GetFileSize(const std::string& fname, uint64_t* file_size);

  // Store the last modification time of fname in *file_mtime.
  Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime);
  // Rename file src to target.
  Status RenameFile(const std::string& src,
                            const std::string& target);
  // Hard Link file src to target.
  Status LinkFile(const std::string& src, const std::string& target) {
    return Status::NotSupported("LinkFile is not supported for this Env");
  }

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  Status LockFile(const std::string& fname, FileLock** lock);

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  Status UnlockFile(FileLock* lock);

  // Priority for scheduling job in thread pool
  enum Priority { LOW, HIGH, TOTAL };

  // Priority for requesting bytes in rate limiter scheduler
  enum IOPriority {
    IO_LOW = 0,
    IO_HIGH = 1,
    IO_TOTAL = 2
  };

  // Arrange to run "(*function)(arg)" once in a background thread, in
  // the thread pool specified by pri. By default, jobs go to the 'LOW'
  // priority thread pool.

  // "function" may run in an unspecified thread.  Multiple functions
  // added to the same Env may run concurrently in different threads.
  // I.e., the caller may not assume that background work items are
  // serialized.
  void Schedule(void (*function)(void* arg), void* arg,
                        Priority pri = LOW, void* tag = nullptr);

  // Arrange to remove jobs for given arg from the queue_ if they are not
  // already scheduled. Caller is expected to have exclusive lock on arg.
  int UnSchedule(void* arg, Priority pri) { return 0; }

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  void StartThread(void (*function)(void* arg), void* arg);

  // Wait for all threads started by StartThread to terminate.
  void WaitForJoin() {}

  // Get thread pool queue length for specific thrad pool.
  unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const {
    return 0;
  }

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  Status GetTestDirectory(std::string* path);

  // Create and return a log file for storing informational messages.
  Status NewLogger(const std::string& fname,
                           shared_ptr<Logger>* result);

  // Returns the number of micro-seconds since some fixed point in time. Only
  // useful for computing deltas of time.
  // However, it is often used as system time such as in GenericRateLimiter
  // and other places so a port needs to return system time in order to work.
  uint64_t NowMicros();

  // Returns the number of nano-seconds since some fixed point in time. Only
  // useful for computing deltas of time in one run.
  // Default implementation simply relies on NowMicros
  uint64_t NowNanos() {
    return NowMicros() * 1000;
  }

  // Sleep/delay the thread for the perscribed number of micro-seconds.
  void SleepForMicroseconds(int micros);

  // Get the current host name.
  Status GetHostName(char* name, uint64_t len);

  // Get the number of seconds since the Epoch, 1970-01-01 00:00:00 (UTC).
  Status GetCurrentTime(int64_t* unix_time);

  // Get full directory name for this db.
  Status GetAbsolutePath(const std::string& db_path,
      std::string* output_path);

  // The number of background worker threads of a specific thread pool
  // for this environment. 'LOW' is the default pool.
  // default number: 1
  void SetBackgroundThreads(int number, Priority pri = LOW);

  // Enlarge number of background worker threads of a specific thread pool
  // for this environment if it is smaller than specified. 'LOW' is the default
  // pool.
  void IncBackgroundThreadsIfNeeded(int number, Priority pri);

  // Lower IO priority for threads from the specified pool.
  void LowerThreadPoolIOPriority(Priority pool = LOW) {}

  // Converts seconds-since-Jan-01-1970 to a printable string
  std::string TimeToString(uint64_t time);

  // Returns the status of all threads that belong to the current Env.
  Status GetThreadList(std::vector<ThreadStatus>* thread_list) {
    return Status::NotSupported("Not supported.");
  }

  BlueRocksEnv(BlueFS *f);
private:
  BlueFS *fs;
};

#endif
