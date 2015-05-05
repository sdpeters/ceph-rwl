// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef LIVE_QUERY_H_
#define LIVE_QUERY_H_

#include "include/filepath.h"
#include "include/encoding.h"
#include "common/Formatter.h"


class LiveQuery {
  public:
    typedef uint64_t id_t;

    class GroupBy {
      public:

        // Group by client ID?
        bool client_id;

        // Group by operation type?
        // TODO variations on this, e.g. group reads vs writes
        bool op_type;

        // TODO: group by arbitrary client metadata property (hostname, job ID)

        void encode(bufferlist &bl) const
        {
          ENCODE_START(1, 1, bl);
          ::encode(client_id, bl);
          ::encode(op_type, bl);
          ENCODE_FINISH(bl);
        }

        void decode(bufferlist::iterator &p)
        {
          DECODE_START(1, p);
          ::decode(client_id, p);
          ::decode(op_type, p);
          DECODE_FINISH(p);
        }
    };

    class Where {
      public:
      // Array of dnames from /
      filepath path_prefix;

      // TODO by client ID, client metadata property, op type
      //

      void encode(bufferlist &bl) const
      {
        ENCODE_START(1, 1, bl);
        ::encode(path_prefix, bl);
        ENCODE_FINISH(bl);
      }

      void decode(bufferlist::iterator &p)
      {
        DECODE_START(1, p);
        ::decode(path_prefix, p);
        DECODE_FINISH(p);
      }

      void parse(const std::string &path);
    };

    // Unique ID, generated on creation
    id_t id;
    // Initially the "SELECT" part is always "op count"
    //select

    // What events contribute to the result values?
    Where where;

    // How to bin the result values?
    GroupBy group_by;

    void encode(bufferlist &bl) const
    {
      ENCODE_START(1, 1, bl);
      ::encode(id, bl);
      where.encode(bl);
      group_by.encode(bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator &p)
    {
      DECODE_START(1, p);
      ::decode(id, p);
      where.decode(p);
      group_by.decode(p);
      DECODE_FINISH(p);
    }

    void dump(Formatter *f) const;
};

WRITE_CLASS_ENCODER(LiveQuery)

class LiveQueryResult
{
  public:
    // TODO define keys such that they can be parsed out again by readers
    typedef std::string GroupKey;
    // XXX generalise to use a PerfCounter instead to get e.g. longrunningavg?
    typedef uint64_t counter_t;

    std::map<GroupKey, counter_t> data;

    void dump(Formatter *f);
};


#endif // LIVE_QUERY_H_

