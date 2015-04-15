/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 * Copyright Inktank 2013
 */

#include "objclass/objclass.h"
#include "global/global_context.h"

#include "cls_replica_log_types.h"
#include "cls_replica_log_ops.h"

CLS_VER(1, 0)
CLS_NAME(replica_log)

cls_handle_t h_class;
cls_method_handle_t h_replica_log_set;
cls_method_handle_t h_replica_log_delete;
cls_method_handle_t h_replica_log_get;
cls_method_handle_t h_replica_log_list;

static const string replica_log_prefix = "rl.";
static const string replica_log_default_bounds = "rl_bounds";

static int get_bounds(cls_method_context_t hctx, const string& key, cls_replica_log_bound& bound)
{
  bufferlist bounds_bl;
  string k;
  if (!key.empty()) {
    k = replica_log_prefix + key;
  } else {
    k = replica_log_default_bounds;
  }
  int rc = cls_cxx_map_get_val(hctx, k, &bounds_bl);
  if (rc < 0) {
    return rc;
  }

  try {
    bufferlist::iterator bounds_bl_i = bounds_bl.begin();
    ::decode(bound, bounds_bl_i);
  } catch (buffer::error& err) {
    bound = cls_replica_log_bound();
    CLS_LOG(0, "ERROR: get_bounds(): failed to decode on-disk bounds object");
    return -EIO;
  }

  return 0;
}

static int write_bounds(cls_method_context_t hctx,
                        const string& key,
                        const cls_replica_log_bound& bound)
{
  bufferlist bounds_bl;
  ::encode(bound, bounds_bl);
  string k;
  if (!key.empty()) {
    k = replica_log_prefix + key;
  } else {
    k = replica_log_default_bounds;
  }
  return cls_cxx_map_set_val(hctx, k, &bounds_bl);
}

static int cls_replica_log_set(cls_method_context_t hctx,
                               bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_replica_log_set_marker_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: cls_replica_log_set(): failed to decode op");
    return -EINVAL;
  }

  cls_replica_log_bound bound;
  int rc = get_bounds(hctx, op.key, bound);
  if (rc < 0 && rc != -ENOENT) {
    return rc;
  }

  rc = bound.update_marker(op.marker);
  if (rc < 0) {
    return rc;
  }

  return write_bounds(hctx, op.key, bound);
}

static int cls_replica_log_delete(cls_method_context_t hctx,
                                  bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_replica_log_delete_marker_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: cls_replica_log_delete(): failed to decode op");
    return -EINVAL;
  }

  cls_replica_log_bound bound;
  int rc = get_bounds(hctx, op.key, bound);
  if (rc < 0 && rc != -ENOENT) {
    return rc;
  }

  rc = bound.delete_marker(op.entity_id);
  if (rc < 0) {
    return rc;
  }

  return write_bounds(hctx, op.key, bound);
}

static int cls_replica_log_get(cls_method_context_t hctx,
                               bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_replica_log_get_bounds_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: cls_replica_log_get(): failed to decode op");
    return -EINVAL;
  }

  cls_replica_log_bound bound;
  int rc = get_bounds(hctx, op.key, bound);
  if (rc < 0) {
    return rc;
  }

  cls_replica_log_get_bounds_ret ret;
  ret.oldest_time = bound.get_oldest_time();
  ret.position_marker = bound.get_lowest_marker_bound();
  bound.get_markers(ret.markers);

  ::encode(ret, *out);
  return 0;
}

static int cls_replica_log_list(cls_method_context_t hctx,
                               bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_replica_log_list_keys_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: cls_replica_log_list(): failed to decode op");
    return -EINVAL;
  }

  cls_replica_log_list_keys_ret ret;

  set<string> raw_keys;

#define MAX_KEYS 100
  string marker;
  if (!op.marker.empty()) {
    marker = replica_log_prefix + op.marker;
  }
  int rc = cls_cxx_map_get_keys(hctx, marker, MAX_KEYS + 1, &raw_keys);
  if (rc < 0) {
    return rc;
  }

  set<string>::iterator iter = raw_keys.begin();
  int i;
  for (i = 0; i < MAX_KEYS && iter != raw_keys.end(); ++i, ++iter) {
    const string& k = *iter;
    if (k.size() < 3) {
      /* should never happen */
      return -EIO;
    }
    if (k[2] == '.') { /* starts with "rl." */
      ret.keys.insert(k.substr(3));
    } else {
      /*
       * we don't want to list the default bound with the keys. The reason is that the default bound
       * serves for a different purpose, and it can be accessed when an empty key is specified,
       * however, this is problematic as it doesn't work well with the marker (an empty marker means
       * we're at the beginning). We can find some hacky solution for that but there really is no
       * reason for that.
       */
      continue;
    }
  }

  ret.is_truncated = (iter != raw_keys.end());

  ::encode(ret, *out);
  return 0;
}

void __cls_init()
{
  CLS_LOG(1, "Loaded replica log class!");

  cls_register("replica_log", &h_class);

  cls_register_cxx_method(h_class, "set", CLS_METHOD_RD | CLS_METHOD_WR,
                          cls_replica_log_set, &h_replica_log_set);
  cls_register_cxx_method(h_class, "get", CLS_METHOD_RD,
                          cls_replica_log_get, &h_replica_log_get);
  cls_register_cxx_method(h_class, "delete", CLS_METHOD_RD | CLS_METHOD_WR,
                          cls_replica_log_delete, &h_replica_log_delete);
  cls_register_cxx_method(h_class, "list", CLS_METHOD_RD,
                          cls_replica_log_list, &h_replica_log_list);
}
