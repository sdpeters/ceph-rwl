/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CLS_REPLICA_LOG_OPS_H_
#define CLS_REPLICA_LOG_OPS_H_

#include "include/types.h"
#include "cls_replica_log_types.h"

struct cls_replica_log_delete_marker_op {
  string key;
  string entity_id;

  cls_replica_log_delete_marker_op() {}
  cls_replica_log_delete_marker_op(const string& k, const string& id) : key(k), entity_id(id) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(entity_id, bl);
    ::encode(key, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(entity_id, bl);
    if (struct_v >= 2) {
      ::decode(key, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<cls_replica_log_delete_marker_op*>& ls);

};
WRITE_CLASS_ENCODER(cls_replica_log_delete_marker_op)

struct cls_replica_log_set_marker_op {
  string key;
  cls_replica_log_progress_marker marker;
  cls_replica_log_set_marker_op() {}
  cls_replica_log_set_marker_op(const string& k, const cls_replica_log_progress_marker& m) :
    key(k), marker(m) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(marker, bl);
    ::encode(key, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(marker, bl);
    if (struct_v >= 2) {
      ::decode(key, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<cls_replica_log_set_marker_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_replica_log_set_marker_op)

struct cls_replica_log_get_bounds_op {
  string key;

  cls_replica_log_get_bounds_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(key, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    if (struct_v >= 2) {
      ::decode(key, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<cls_replica_log_get_bounds_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_replica_log_get_bounds_op)

struct cls_replica_log_get_bounds_ret {
  string position_marker; // oldest log listing position on the master
  utime_t oldest_time; // oldest timestamp associated with position or an item
  std::list<cls_replica_log_progress_marker> markers;

  cls_replica_log_get_bounds_ret() {}
  cls_replica_log_get_bounds_ret(const string& pos_marker,
    const utime_t& time,
    const std::list<cls_replica_log_progress_marker>& m) :
    position_marker(pos_marker), oldest_time(time), markers(m)
  {}
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(position_marker, bl);
    ::encode(oldest_time, bl);
    ::encode(markers, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(position_marker, bl);
    ::decode(oldest_time, bl);
    ::decode(markers, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<cls_replica_log_get_bounds_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_replica_log_get_bounds_ret)

struct cls_replica_log_list_keys_op {
  string marker;

  cls_replica_log_list_keys_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    if (struct_v >= 1) {
      ::decode(marker, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<cls_replica_log_list_keys_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_replica_log_list_keys_op)

struct cls_replica_log_list_keys_ret {
  std::set<string> keys;
  bool is_truncated;

  cls_replica_log_list_keys_ret() : is_truncated(false) {}
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(keys, bl);
    ::encode(is_truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(keys, bl);
    ::decode(is_truncated, bl);
    DECODE_FINISH(bl);
  }

  static void generate_test_instances(std::list<cls_replica_log_list_keys_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_replica_log_list_keys_ret)


#endif /* CLS_REPLICA_LOG_OPS_H_ */
