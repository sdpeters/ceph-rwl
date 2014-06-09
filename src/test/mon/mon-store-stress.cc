// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2014 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <time.h>
#include <iostream>
#include <stdlib.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include "mon/MonitorDBStore.h"
#include "include/stringify.h"
#include "include/assert.h"

typedef boost::mt11213b rngen_t;

#define dout_subsys ceph_subsys_

void apply_tx(MonitorDBStore *db, MonitorDBStore::Transaction *tx,
              long long *num_ops, double timeout = 0.0)
{
  utime_t s = ceph_clock_now(NULL);
  db->apply_transaction(*tx);
  utime_t e = ceph_clock_now(NULL);
  (*num_ops)++;
  std::cout << "[" << *num_ops << "] "
            << "apply: " << (e - s) << " sec" << std::endl;
  if (timeout > 0 && (e - s) >= timeout) {
    std::cerr << "tx took " << (e - s) << " secs (timeout: "
              << timeout << " secs)" << std::endl;
    assert(0 == "too long to apply tx");
  }
}

void usage(string n)
{
  std::cout << "usage: " << n << " [options] <path>\n"
            << "options:\n"
            << " --test-seed <VAL>           test seed\n"
            << " --keep-state                keep existing store state\n"
            << " --trim-from-zero            trim state from zero to current\n"
            << " --trim-period <VAL>         trim period (default: 2.0 [secs])\n"
            << " --stop-at <N>               stop at (of after) Nth operation\n"
            << " --list                      list store contents\n"
            << " --percent-read <VAL>        amount of reads (def: 40)\n"
            << " --percent-read-range <VAL>  amound of read-range ops (def: 60)\n"
            << " --percent-read-single <VAL> amount of single read ops (def: 30)\n"
            << " --percent-read-iter <VAL>   amount of read-iter ops (def: 10)\n"
            << " --percent-write <VAL>       amount of write ops (def: 60)\n"
            << " --write-min-size <VAL>      min write size (def: 40K)\n"
            << " --write-max-size <VAL>      max write size (def: 500K)\n"
            << " --num-writes-per-compact N  num writes per compaction\n"
            << " --write-timeout <FLOAT>     timeout/die for writes (def: disabled)\n"
            << " --help                      this\n" << std::endl;
}

void trim(MonitorDBStore &db,
          map<string,version_t> &fc,
          map<string,version_t> &lc,
          string prefix,
          bool trim_from_start,
          long long *num_ops, double write_timeout)
{
  version_t start = fc[prefix];
  if (trim_from_start)
    start = 1;

  if (lc[prefix] - start > 500) {
    // trim
    version_t end = MIN(lc[prefix] + 500,
        lc[prefix] - g_conf->paxos_trim_max);
    std::cout << "wtrim: prefix = " << prefix
      << ", fc: " << start << ", end: " << end
      << std::endl;
    MonitorDBStore::Transaction trim_tx;
    for (version_t v = start; v < end; ++v) {
      trim_tx.erase(prefix, v);
    }
    trim_tx.put(prefix, "first_committed", end);
    trim_tx.compact_range(prefix, stringify(start-1), stringify(end));
    apply_tx(&db, &trim_tx, num_ops, write_timeout);
    fc[prefix] = end;
  }
}

void trim(MonitorDBStore &db, long long *num_ops, double write_timeout)
{
  std::cout << "full compact" << std::endl;
  utime_t s = ceph_clock_now(NULL);
  db.compact();
  utime_t e = ceph_clock_now(NULL);
  std::cout << "full_compact: " << (e - s) << " secs" << std::endl;
  if (write_timeout > 0.0 && write_timeout <= (e-s)) {
    std::cerr << "full_compact took longer than timeout "
              << write_timeout << " secs" << std::endl;
    assert(0 == "compaction took too long!");
  }
}


int main(int argc, const char *argv[])
{
  vector<const char*> def_args;
  vector<const char*> args;
  string our_name(argv[0]);
  argv_to_vec(argc, argv, args);

  global_init(&def_args, args,
	      CEPH_ENTITY_TYPE_MON,
              CODE_ENVIRONMENT_UTILITY,
	      0);

  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);


  uint64_t seed = (uint64_t) time(NULL);
  string path;
  bool list = false;
  bool keep_state = false;
  bool trim_from_zero = false;
  long long stop_at = -1;

  int percent_read = -1;
  int percent_read_range = -1;
  int percent_read_single = -1;
  int percent_read_iter = -1;
  int percent_write = -1;
  double trim_period = -1;
  uint64_t write_min_size = 0;
  uint64_t write_max_size = 0;
  int num_writes_per_compact = -1;
  double write_timeout = 0.0;

  for (std::vector<const char*>::iterator p = args.begin();
       p != args.end();) {
    string val;
    string err;
    if (ceph_argparse_double_dash(args, p)) {
      break;
    } else if (ceph_argparse_witharg(args, p, &val,
               "--test-seed", (char*)NULL)) {
      seed = (uint64_t) strict_strtoll(val.c_str(), 10, &err);
    } else if (ceph_argparse_flag(args, p, "--list", (char*)NULL)) {
      list = true;
    } else if (ceph_argparse_flag(args, p, "--keep-state", (char*)NULL)) {
      keep_state = true;
    } else if (ceph_argparse_flag(args, p, "--trim-from-zero", (char*)NULL)) {
      trim_from_zero = true;
    } else if (ceph_argparse_flag(args, p, "--help", (char*)NULL)) {
      usage(our_name);
      return 0;
    } else if (ceph_argparse_witharg(args, p, &val,
               "--percent-read", (char*)NULL)) {
      percent_read = (int) strict_strtol(val.c_str(), 10, &err);
    } else if (ceph_argparse_witharg(args, p, &val,
               "--percent-read-range", (char*)NULL)) {
      percent_read_range = (int) strict_strtol(val.c_str(), 10, &err);
    } else if (ceph_argparse_witharg(args, p, &val,
               "--percent-read-single", (char*)NULL)) {
      percent_read_single = (int) strict_strtol(val.c_str(), 10, &err);
    } else if (ceph_argparse_witharg(args, p, &val,
               "--percent-read-iter", (char*)NULL)) {
      percent_read_iter = (int) strict_strtol(val.c_str(), 10, &err);
    } else if (ceph_argparse_witharg(args, p, &val,
               "--percent-write", (char*)NULL)) {
      percent_write = (int) strict_strtol(val.c_str(), 10, &err);
    } else if (ceph_argparse_witharg(args, p, &val,
               "--trim-period", (char*)NULL)) {
      trim_period = strict_strtod(val.c_str(), &err);
    } else if (ceph_argparse_witharg(args, p, &val,
               "--stop-at", (char*)NULL)) {
      stop_at = strict_strtoll(val.c_str(), 10, &err);
    } else if (ceph_argparse_witharg(args, p, &val,
              "--write-min-size", (char*)NULL)) {
      write_min_size = strict_sistrtoll(val.c_str(), &err);
    } else if (ceph_argparse_witharg(args, p, &val,
              "--write-max-size", (char*)NULL)) {
      write_max_size = strict_sistrtoll(val.c_str(), &err);
    } else if (ceph_argparse_witharg(args, p, &val,
               "--num-writes-per-compact", (char*)NULL)) {
      num_writes_per_compact = strict_strtol(val.c_str(), 10, &err);
    } else if (ceph_argparse_witharg(args, p, &val,
               "--write-timeout", (char*)NULL)) {
      write_timeout = strict_strtod(val.c_str(), &err);
    } else {
      if (path.empty()) {
        path = *p++;
      } else {
        std::cerr << "invalid argument: " << *p << std::endl;
        usage(our_name);
        return EINVAL;
      }
    }

    if (!err.empty()) {
      std::cerr << "error parsing '" << *p << "': " << err << std::endl;
      return EINVAL;
    }
  }

  if (trim_period < 0) {
    trim_period = 2.0;
  }

  if (percent_read > 0 && percent_write < 0) {
    percent_write = 100 - percent_read;
  } else if (percent_write > 0 && percent_read < 0) {
    percent_read = 100 - percent_write;
  } else if (percent_read < 0 && percent_write < 0) {
    percent_read = 40;
    percent_write = 60;
  }
  if (percent_read + percent_write > 100) {
    std::cerr << "error: percent read + percent write must be < 100"
              << std::endl;
    return EINVAL;
  }

  if (percent_read_range < 0) {
    percent_read_range = 60;
  }
  if (percent_read_single < 0) {
    percent_read_single = 30;
  }
  if (percent_read_iter < 0) {
    percent_read_iter = 10;
  }

  if (percent_read_range + percent_read_single + percent_read_iter > 100) {
    std::cerr << "error: percents for read range, read single and read iter"
              << " must be < 100" << std::endl;
    return EINVAL;
  }

  if (path.empty()) {
    std::cerr << "must specify store path" << std::endl;
    usage(our_name);
    return EINVAL;
  }

  std::cout << "hammer db (seed: " << seed << ")" << std::endl;

  MonitorDBStore db(path);
  int r = db.create_and_open(std::cerr);
  if (r < 0) {
    std::cerr << "error creating store at " << path << std::endl;
    return -r;
  }

  std::cout << "hammer db" << std::endl;

  string lc_str("last_committed");
  string fc_str("first_committed");
  string paxos_str("paxos");

  vector<string> prefixes;
  prefixes.push_back("osdmap");
  prefixes.push_back("pgmap");
  prefixes.push_back("pgmap_foo");
  prefixes.push_back("pgmap_bar");
  prefixes.push_back("pgmap_baz");
  prefixes.push_back("pgmap_temp");
  map<string,version_t> prefix_fc;
  map<string,version_t> prefix_lc;
  for (vector<string>::iterator p = prefixes.begin();
       p != prefixes.end(); ++p) {
    prefix_fc[*p] = 0;
    prefix_lc[*p] = 0;
  }
  prefix_fc[paxos_str] = 0;
  prefix_lc[paxos_str] = 0;

  if (keep_state) {
    set<string> t_prefixes(prefixes.begin(), prefixes.end());
    t_prefixes.insert(paxos_str);
    for (set<string>::iterator p = t_prefixes.begin();
         p != t_prefixes.end(); ++p) {
      prefix_lc[*p] = db.get(*p, lc_str);
      prefix_fc[*p] = db.get(*p, fc_str);
      if (prefix_fc[*p] == 0)
        prefix_fc[*p] = 1;
      std::cout << "init keep-state: prefix = " << *p
                << ", fc: " << prefix_fc[*p] << ", lc: " << prefix_lc[*p]
                << std::endl;
    }
  }

  rngen_t gen(seed);
  boost::uniform_int<> op_rng(0,100);
  boost::uniform_int<> pref_rng(0,prefixes.size()-1);
  boost::uniform_int<> write_size_rng(write_min_size, write_max_size);
  boost::uniform_int<> char_rng(0,127);

  if (list) {
    KeyValueDB::WholeSpaceIterator iter = db.get_iterator();
    iter->seek_to_first();
    while (iter->valid()) {
      pair<string,string> rk = iter->raw_key();
      std::cout << "list: prefix = " << rk.first
                << ", key = " << rk.second << std::endl;
      iter->next();
    }
    return 0;
  }

  utime_t last_trim;
  long long num_ops = 0;
  int writes_since_compact = 0;

  while (true) {
    string prefix = prefixes[pref_rng(gen)];
    int p = op_rng(gen);

    if (percent_write > 0 && p > (100-percent_write)) { // write
      version_t v = ++prefix_lc[prefix];
      uint64_t s = write_size_rng(gen);
      bufferptr bp(s);
      bufferlist bl;
      for (uint64_t i = 0; i < s; ++i) {
        bp[i] = char_rng(gen);
      }
      bl.append(bp);

      bufferlist tx_bl;

      MonitorDBStore::Transaction t;
      t.put(prefix, v, bl);
      if (prefix_fc.count(prefix) == 0) {
        t.put(prefix, fc_str, v);
        prefix_fc[prefix] = v;
      }
      t.put(prefix, lc_str, v);
      t.encode(tx_bl);

      MonitorDBStore::Transaction paxos_tx;
      version_t paxos_v = ++prefix_lc[paxos_str];
      paxos_tx.put(paxos_str, paxos_v, tx_bl);
      paxos_tx.put(paxos_str, lc_str, paxos_v);
      if (prefix_fc.count(paxos_str) == 0) {
        prefix_fc[paxos_str] = paxos_v;
        paxos_tx.put(paxos_str, fc_str, paxos_v);
      }
      std::cout << "write: prefix = " << prefix << ", key = " << v
                << ", size = " << s << std::endl;
      paxos_tx.append(t);
//      apply_tx(&db, &t, &num_ops, write_timeout);
      apply_tx(&db, &paxos_tx, &num_ops, write_timeout);
      writes_since_compact ++;

    } else if (percent_read > 0) { // read
      // full iteration || single read || range read
      int read_p = op_rng(gen);

      utime_t s = ceph_clock_now(NULL);

      if (percent_read_range > 0 && read_p > (100-percent_read_range)) {
        std::cout << "rread: prefix = " << prefix
          << ", fc = " << prefix_fc[prefix]
          << ", lc = " << prefix_lc[prefix] << std::endl;
        for (version_t i = prefix_fc[prefix]; i < prefix_lc[prefix]; ++i) {
          bufferlist bl;
          db.get(prefix, i, bl);
        }
      } else if (percent_read_single > 0 && read_p > (100-percent_read_single)) {
        boost::uniform_int<> sread_rng(prefix_fc[prefix],prefix_lc[prefix]);
        version_t v = sread_rng(gen);
        std::cout << "sread: prefix = " << prefix << ", v = " << v << std::endl;
        bufferlist bl;
        db.get(prefix, v, bl);
      } else if (percent_read_iter > 0 && read_p > (100-percent_read_iter)) {
        uint64_t total_keys = 0;
        for (vector<string>::iterator p = prefixes.begin();
            p != prefixes.end(); ++p) {
          total_keys += prefix_lc[*p] - prefix_fc[*p];
        }
        total_keys += prefix_lc[paxos_str] - prefix_fc[paxos_str];
        std::cout << "fiter: total_keys: " << total_keys << std::endl;

        pair<string,string> start;
        set<string> sync_prefixes(prefixes.begin(), prefixes.end());
        sync_prefixes.insert(paxos_str);
        MonitorDBStore::Synchronizer iter = db.get_synchronizer(start, sync_prefixes);

        while (iter->has_next_chunk()) {
          pair<string,string> k = iter->get_next_key();
          bufferlist bl;
          db.get(k.first, k.second, bl);
        }
      }

      utime_t e = ceph_clock_now(NULL);
      std::cout << "[" << (++num_ops) << "] "
                << "rtime: " << (e - s) << " sec" << std::endl;
    }

    if (ceph_clock_now(NULL) - last_trim > trim_period) {
      std::cout << "wtrim" << std::endl;
      for (vector<string>::iterator p = prefixes.begin();
           p != prefixes.end(); ++p) {
        trim(db, prefix_fc, prefix_lc, *p, trim_from_zero, &num_ops, write_timeout);
      }
      trim(db, prefix_fc, prefix_lc, paxos_str, trim_from_zero, &num_ops, write_timeout);
      last_trim = ceph_clock_now(NULL);
    }
    if (num_writes_per_compact > 0 &&
        writes_since_compact >= num_writes_per_compact) {
      trim(db, &num_ops, write_timeout);
      writes_since_compact = 0;
    }

    if (stop_at > 0 && stop_at >= num_ops) {
      std::cout << "ran for " << num_ops << " ops; stop." << std::endl;
      break;
    }
  }

  return 0;
}
