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


#include "LiveQuery.h"

void LiveQuery::Where::parse(const std::string &path)
{
  path_prefix.set_path(path.c_str());
}

void LiveQueryResult::dump(Formatter *f)
{
  f->open_object_section("result_groups");
  for (std::map<GroupKey, counter_t>::iterator i = data.begin();
       i != data.end(); ++i) {
    f->dump_int(i->first.c_str(), i->second);
  }
  f->close_section();
}

