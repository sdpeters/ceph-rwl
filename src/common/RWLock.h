// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#ifndef CEPH_RWLock_Posix__H
#define CEPH_RWLock_Posix__H

#include <string>
#include <pthread.h>
#include "lockdep.h"
#include "include/stringify.h"

class RWLock
{
  mutable pthread_rwlock_t L;
  int id;
  std::string name;

public:
  RWLock(const RWLock& other);
  const RWLock& operator=(const RWLock& other);

  RWLock(const char *n) : id(-1) {
    pthread_rwlock_init(&L, NULL);
    name = std::string(n) + "-" + stringify(this);
    if (g_lockdep) id = lockdep_register(name.c_str());
  }

  virtual ~RWLock() {
    pthread_rwlock_unlock(&L);
    pthread_rwlock_destroy(&L);
  }

  void unlock() {
    if (g_lockdep) id = lockdep_will_unlock(name.c_str(), id);
    pthread_rwlock_unlock(&L);
  }

  // read
  void get_read(bool no_lockdep=false) {
    //if (g_lockdep && !no_lockdep) id = lockdep_will_lock(name.c_str(), id);
    pthread_rwlock_rdlock(&L);
    //if (g_lockdep && !no_lockdep) id = lockdep_locked(name.c_str(), id);
  }
  bool try_get_read(bool no_lockdep=false) {
    if (pthread_rwlock_tryrdlock(&L) == 0) {
      if (g_lockdep && !no_lockdep) id = lockdep_locked(name.c_str(), id);
      return true;
    }
    return false;
  }
  void put_read() {
    unlock();
  }

  // write
  void get_write(bool no_lockdep=false) {
    if (g_lockdep && !no_lockdep) id = lockdep_will_lock(name.c_str(), id);
    pthread_rwlock_wrlock(&L);
    if (g_lockdep && !no_lockdep) id = lockdep_locked(name.c_str(), id);
  }
  bool try_get_write(bool no_lockdep=false) {
    if (pthread_rwlock_trywrlock(&L) == 0) {
      if (g_lockdep && !no_lockdep) id = lockdep_locked(name.c_str(), id);
      return true;
    }
    return false;
  }
  void put_write() {
    unlock();
  }

public:
  class RLocker {
    RWLock &m_lock;

  public:
    RLocker(RWLock& lock) : m_lock(lock) {
      m_lock.get_read();
    }
    ~RLocker() {
      m_lock.put_read();
    }
  };

  class WLocker {
    RWLock &m_lock;

  public:
    WLocker(RWLock& lock) : m_lock(lock) {
      m_lock.get_write();
    }
    ~WLocker() {
      m_lock.put_write();
    }
  };
};

#endif // !_Mutex_Posix_
