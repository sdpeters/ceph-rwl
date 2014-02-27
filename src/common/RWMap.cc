
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

  bool get_entry(const K& k, bool for_write, void **handle, V **v) {
    rwlock.get_read();
    typename std::map<K, V>::iterator iter = m.find(k);
    if (iter == m.end()) {
      rwlock.unlock();
      return false;
    }
    RWMapEntry& entry = iter->second;
    *handle = &entry;
    if (for_write) {
      entry.lock.get_write();
    } else {
      entry.lock.get_read();
    }

    bool ret = true;
    if (entry.removing) {
      ret = false;
      entry.lock.unlock();
      goto done;
    }

    if (v) {
      *v = &entry.data;
    }

done:
    rwlock.unlock();
    return ret;
  }

public:
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
