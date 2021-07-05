// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_TYPES_H
#define CEPH_LIBRBD_IO_TYPES_H

#include "include/int_types.h"
#include "osdc/StriperTypes.h"
#include <map>
#include <vector>

namespace librbd {
namespace io {

#define RBD_QOS_IOPS_THROTTLE				1 << 0
#define RBD_QOS_BPS_THROTTLE				1 << 1
#define RBD_QOS_READ_IOPS_THROTTLE			1 << 2
#define RBD_QOS_WRITE_IOPS_THROTTLE			1 << 3
#define RBD_QOS_READ_BPS_THROTTLE			1 << 4
#define RBD_QOS_WRITE_BPS_THROTTLE			1 << 5

#define RBD_QOS_BPS_MASK	(RBD_QOS_BPS_THROTTLE | RBD_QOS_READ_BPS_THROTTLE | RBD_QOS_WRITE_BPS_THROTTLE)
#define RBD_QOS_IOPS_MASK	(RBD_QOS_IOPS_THROTTLE | RBD_QOS_READ_IOPS_THROTTLE | RBD_QOS_WRITE_IOPS_THROTTLE)
#define RBD_QOS_READ_MASK	(RBD_QOS_READ_BPS_THROTTLE | RBD_QOS_READ_IOPS_THROTTLE)
#define RBD_QOS_WRITE_MASK	(RBD_QOS_WRITE_BPS_THROTTLE | RBD_QOS_WRITE_IOPS_THROTTLE)

#define RBD_QOS_MASK		(RBD_QOS_BPS_MASK | RBD_QOS_IOPS_MASK)

typedef enum {
  AIO_TYPE_NONE = 0,
  AIO_TYPE_GENERIC,
  AIO_TYPE_OPEN,
  AIO_TYPE_CLOSE,
  AIO_TYPE_READ,
  AIO_TYPE_WRITE,
  AIO_TYPE_DISCARD,
  AIO_TYPE_FLUSH,
  AIO_TYPE_WRITESAME,
  AIO_TYPE_COMPARE_AND_WRITE,
} aio_type_t;

enum FlushSource {
  FLUSH_SOURCE_USER,
  FLUSH_SOURCE_INTERNAL,
  FLUSH_SOURCE_SHUTDOWN,
  FLUSH_SOURCE_WRITEBACK
};

enum Direction {
  DIRECTION_READ,
  DIRECTION_WRITE,
  DIRECTION_BOTH
};

enum DispatchResult {
  DISPATCH_RESULT_INVALID,
  DISPATCH_RESULT_CONTINUE,
  DISPATCH_RESULT_COMPLETE
};

enum ObjectDispatchLayer {
  OBJECT_DISPATCH_LAYER_NONE = 0,
  OBJECT_DISPATCH_LAYER_CACHE,
  OBJECT_DISPATCH_LAYER_JOURNAL,
  OBJECT_DISPATCH_LAYER_SCHEDULER,
  OBJECT_DISPATCH_LAYER_CORE,
  OBJECT_DISPATCH_LAYER_LAST
};

enum {
  OBJECT_DISCARD_FLAG_DISABLE_CLONE_REMOVE      = 1UL << 0,
  OBJECT_DISCARD_FLAG_DISABLE_OBJECT_MAP_UPDATE = 1UL << 1
};

enum {
  OBJECT_DISPATCH_FLAG_FLUSH                    = 1UL << 0,
  OBJECT_DISPATCH_FLAG_WILL_RETRY_ON_ERROR      = 1UL << 1
};

using striper::LightweightBufferExtents;
using striper::LightweightObjectExtent;
using striper::LightweightObjectExtents;

typedef std::vector<std::pair<uint64_t, uint64_t> > Extents;
typedef std::map<uint64_t, uint64_t> ExtentMap;

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_TYPES_H
