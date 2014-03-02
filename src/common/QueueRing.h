#ifndef QUEUE_RING_H
#define QUEUE_RING_H

#include <list>
#include "common/Mutex.h"
#include "common/Cond.h"



template <class T>
class QueueRing {
  struct QueueBucket {
    Mutex lock;
    Cond cond;
    typename std::list<T> entries;

    QueueBucket() : lock("QueueRing::QueueBucket::lock") {}

    void enqueue(const T& entry) {
      lock.Lock();
      bool need_signal = entries.empty();
      entries.push_back(entry);

      if (need_signal) {
        cond.Signal();
      }

      lock.Unlock();
    }

    void dequeue(T *entry) {
      lock.Lock();
      if (entries.empty()) {
        cond.Wait(lock);
      };
      assert(!entries.empty());
      *entry = entries.front();
      entries.pop_front();
      lock.Unlock();
    };
  };

  QueueBucket *buckets;
  int num_buckets;
  atomic_t cur_read_bucket;
  atomic_t cur_write_bucket;
public:
  QueueRing(int n) : num_buckets(n) {
    buckets = new QueueBucket[n];
  }

  void enqueue(const T& entry) {
    buckets[cur_write_bucket.inc() % num_buckets].enqueue(entry);
  };

  void dequeue(T *entry) {
    buckets[cur_read_bucket.inc() % num_buckets].dequeue(entry);
  }
};

#endif
