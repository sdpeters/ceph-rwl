#ifndef CEPH_RWMAP_H
#define CEPH_RWMAP_H

#include <map>

#include "common/RWLock.h"

template <class K, class V>
class RWMap {
  RWLock rwlock;

  struct RWMapEntry {
    RWLock lock;
    bool removing;
    V data;

    RWMapEntry() : lock("RWMap::RWMapEntry::m[]"), removing(false) {}
  };

  std::map<K, RWMapEntry> m;

  bool _get_entry(const K& k, bool for_write, void **handle, V **v) {
    typename std::map<K, V>::iterator iter = m.find(k);
    if (iter == m.end()) {
      return false;
    }
    RWMapEntry& entry = iter->second;
    *handle = &entry;
    if (for_write) {
      entry.lock.get_write();
    } else {
      entry.lock.get_read();
    }

    if (entry.removing) {
      entry.lock.unlock();
      return false;
    }

    if (v) {
      *v = &entry.data;
    }

    return true;
  }

  bool get_entry(const K& k, bool for_write, void **handle, V **v) {
    rwlock.get_read();
    bool found = _get_entry(k, for_write, handle, v);
    rwlock.unlock();
    return found;
  }

public:
  RWMap() : rwlock("RWMap") {}
  virtual ~RWMap();
  void get_or_create(const K& k, bool for_write, void **handle, V **v, void *create_params) {
    bool found = get_entry(k, for_write, &handle, v);
    if (found) {
      return;
    }
    
    rwlock.get_write();
    found = _get_entry(k, for_write, &handle, v);
    if (found) {
      rwlock.unlock();
      return;
    }
    alloc_new(v, create_params);
    RWMapEntry& entry = m[k];
    *handle = &entry;

    entry.v = *v;
    if (for_write) {
      entry.lock.get_write();
    } else {
      entry.lock.get_read();
    }

    rwlock.unlock();
  };

  virtual void alloc_new(V **v) {
    *v = new V;
  }

  void set(const K& k, V& v) {
    rwlock.get_write();

    RWMapEntry& entry = m[k];
    entry.data = v;

    rwlock.unlock();
  }

  bool get_read(const K& k, void **handle, V **v) {
    return get_entry(k, false, handle, v);
  }

  bool get_write(const K& k, void **handle, V **v) {
    return get_entry(k, true, handle, v);
  }

  void unlock(void *handle) {
    RWMapEntry *entry = *handle;
    entry->lock.unlock();
  }

  bool remove(const K& k) {
    /*
     * we do it in two stages. First we get_write() which only takes the map's read lock. That way
     * we minimize the probability of holding the map's write lock while waiting on the write lock
     * for the entry.
     */
    void *handle;
    if (!get_write(k, &handle, NULL)) {
      return false;
    }
    RWMapEntry *entry = (RWMapEntry *)handle;
    entry->removing = true;

    unlock(handle);


    rwlock.get_write();
    typename std::map<K, V>::iterator iter = m.find(k);
    assert(iter != m.end());

    m.erase(iter);
    rwlock.unlock();

    return true;
  }

  class RLocker {
    RWMap *rwmap;
    bool valid;
    void *handle;
    V *v;
  public:
    RLocker(RWMap *m, K& k) : rwmap(m) {
      valid = rwmap->get_read(k, &handle, &v);
    }
    ~RLocker() {
      if (valid) {
        rwmap->unlock(handle);
      }
    }
    V *operator*() {
      if (!valid) {
        return NULL;
      }
      return v;
    }
  };

};

#endif
