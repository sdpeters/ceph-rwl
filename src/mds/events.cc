// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"

#include "events/ESubtreeMap.h"
#include "events/ESession.h"
#include "events/ESessions.h"

#include "events/EMetaBlob.h"
#include "events/EResetJournal.h"
#include "events/ENoOp.h"

#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"

#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"

#include "events/ETableClient.h"
#include "events/ETableServer.h"


EMetaBlob::EMetaBlob(MDLog *mdlog) : opened_ino(0), renamed_dirino(0),
				     inotablev(0), sessionmapv(0), allocated_ino(0),
				     last_subtree_map(0), event_seq(0)
{ }

// EMetaBlob::fullbit

void EMetaBlob::fullbit::encode(bufferlist& bl) const {
  ENCODE_START(6, 5, bl);
  ::encode(dn, bl);
  ::encode(dnfirst, bl);
  ::encode(dnlast, bl);
  ::encode(dnv, bl);
  ::encode(inode, bl);
  ::encode(xattrs, bl);
  if (inode.is_symlink())
    ::encode(symlink, bl);
  if (inode.is_dir()) {
    ::encode(dirfragtree, bl);
    ::encode(snapbl, bl);
  }
  ::encode(state, bl);
  if (old_inodes.empty()) {
    ::encode(false, bl);
  } else {
    ::encode(true, bl);
    ::encode(old_inodes, bl);
  }
  ENCODE_FINISH(bl);
}

void EMetaBlob::fullbit::decode(bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(6, 5, 5, bl);
  ::decode(dn, bl);
  ::decode(dnfirst, bl);
  ::decode(dnlast, bl);
  ::decode(dnv, bl);
  ::decode(inode, bl);
  ::decode(xattrs, bl);
  if (inode.is_symlink())
    ::decode(symlink, bl);
  if (inode.is_dir()) {
    ::decode(dirfragtree, bl);
    ::decode(snapbl, bl);
    if ((struct_v == 2) || (struct_v == 3)) {
      bool dir_layout_exists;
      ::decode(dir_layout_exists, bl);
      if (dir_layout_exists) {
	__u8 dir_struct_v;
	::decode(dir_struct_v, bl); // default_file_layout version
	::decode(inode.layout, bl); // and actual layout, that we care about
      }
    }
  }
  if (struct_v >= 6) {
    ::decode(state, bl);
  } else {
    bool dirty;
    ::decode(dirty, bl);
    state = dirty ? EMetaBlob::fullbit::STATE_DIRTY : 0;
  }

  if (struct_v >= 3) {
    bool old_inodes_present;
    ::decode(old_inodes_present, bl);
    if (old_inodes_present) {
      ::decode(old_inodes, bl);
    }
  }
  DECODE_FINISH(bl);
}

void EMetaBlob::fullbit::dump(Formatter *f) const
{
  f->dump_string("dentry", dn);
  f->dump_stream("snapid.first") << dnfirst;
  f->dump_stream("snapid.last") << dnlast;
  f->dump_int("dentry version", dnv);
  f->open_object_section("inode");
  inode.dump(f);
  f->close_section(); // inode
  f->open_array_section("xattrs");
  for (map<string, bufferptr>::const_iterator iter = xattrs.begin();
      iter != xattrs.end(); ++iter) {
    f->dump_string(iter->first.c_str(), iter->second.c_str());
  }
  f->close_section(); // xattrs
  if (inode.is_symlink()) {
    f->dump_string("symlink", symlink);
  }
  if (inode.is_dir()) {
    f->dump_stream("frag tree") << dirfragtree;
    f->dump_string("has_snapbl", snapbl.length() ? "true" : "false");
    if (inode.has_layout()) {
      f->open_object_section("file layout policy");
      // FIXME
      f->dump_string("layout", "the layout exists");
      f->close_section(); // file layout policy
    }
  }
  f->dump_string("state", state_string());
  if (!old_inodes.empty()) {
    f->open_array_section("old inodes");
    for (old_inodes_t::const_iterator iter = old_inodes.begin();
	iter != old_inodes.end(); ++iter) {
      f->open_object_section("inode");
      f->dump_int("snapid", iter->first);
      iter->second.dump(f);
      f->close_section(); // inode
    }
    f->close_section(); // old inodes
  }
}

void EMetaBlob::fullbit::generate_test_instances(list<EMetaBlob::fullbit*>& ls)
{
  inode_t inode;
  fragtree_t fragtree;
  map<string,bufferptr> empty_xattrs;
  bufferlist empty_snapbl;
  fullbit *sample = new fullbit("/testdn", 0, 0, 0,
                                inode, fragtree, empty_xattrs, "", empty_snapbl,
                                false, NULL);
  ls.push_back(sample);
}


// EMetaBlob::remotebit

void EMetaBlob::remotebit::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(dn, bl);
  ::encode(dnfirst, bl);
  ::encode(dnlast, bl);
  ::encode(dnv, bl);
  ::encode(ino, bl);
  ::encode(d_type, bl);
  ::encode(dirty, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::remotebit::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(dn, bl);
  ::decode(dnfirst, bl);
  ::decode(dnlast, bl);
  ::decode(dnv, bl);
  ::decode(ino, bl);
  ::decode(d_type, bl);
  ::decode(dirty, bl);
  DECODE_FINISH(bl);
}

void EMetaBlob::remotebit::dump(Formatter *f) const
{
  f->dump_string("dentry", dn);
  f->dump_int("snapid.first", dnfirst);
  f->dump_int("snapid.last", dnlast);
  f->dump_int("dentry version", dnv);
  f->dump_int("inodeno", ino);
  uint32_t type = DTTOIF(d_type) & S_IFMT; // convert to type entries
  string type_string;
  switch(type) {
  case S_IFREG:
    type_string = "file"; break;
  case S_IFLNK:
    type_string = "symlink"; break;
  case S_IFDIR:
    type_string = "directory"; break;
  case S_IFIFO:
    type_string = "fifo"; break;
  case S_IFCHR:
    type_string = "chr"; break;
  case S_IFBLK:
    type_string = "blk"; break;
  case S_IFSOCK:
    type_string = "sock"; break;
  default:
    assert (0 == "unknown d_type!");
  }
  f->dump_string("d_type", type_string);
  f->dump_string("dirty", dirty ? "true" : "false");
}

void EMetaBlob::remotebit::
generate_test_instances(list<EMetaBlob::remotebit*>& ls)
{
  remotebit *remote = new remotebit("/test/dn", 0, 10, 15, 1, IFTODT(S_IFREG), false);
  ls.push_back(remote);
}

// EMetaBlob::nullbit

void EMetaBlob::nullbit::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(dn, bl);
  ::encode(dnfirst, bl);
  ::encode(dnlast, bl);
  ::encode(dnv, bl);
  ::encode(dirty, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::nullbit::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(dn, bl);
  ::decode(dnfirst, bl);
  ::decode(dnlast, bl);
  ::decode(dnv, bl);
  ::decode(dirty, bl);
  DECODE_FINISH(bl);
}

void EMetaBlob::nullbit::dump(Formatter *f) const
{
  f->dump_string("dentry", dn);
  f->dump_int("snapid.first", dnfirst);
  f->dump_int("snapid.last", dnlast);
  f->dump_int("dentry version", dnv);
  f->dump_string("dirty", dirty ? "true" : "false");
}

void EMetaBlob::nullbit::generate_test_instances(list<nullbit*>& ls)
{
  nullbit *sample = new nullbit("/test/dentry", 0, 10, 15, false);
  nullbit *sample2 = new nullbit("/test/dirty", 10, 20, 25, true);
  ls.push_back(sample);
  ls.push_back(sample2);
}

// EMetaBlob::dirlump

void EMetaBlob::dirlump::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(fnode, bl);
  ::encode(state, bl);
  ::encode(nfull, bl);
  ::encode(nremote, bl);
  ::encode(nnull, bl);
  _encode_bits();
  ::encode(dnbl, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::dirlump::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl)
  ::decode(fnode, bl);
  ::decode(state, bl);
  ::decode(nfull, bl);
  ::decode(nremote, bl);
  ::decode(nnull, bl);
  ::decode(dnbl, bl);
  dn_decoded = false;      // don't decode bits unless we need them.
  DECODE_FINISH(bl);
}

void EMetaBlob::dirlump::dump(Formatter *f) const
{
  if (!dn_decoded) {
    dirlump *me = const_cast<dirlump*>(this);
    me->_decode_bits();
  }
  f->open_object_section("fnode");
  fnode.dump(f);
  f->close_section(); // fnode
  f->dump_string("state", state_string());
  f->dump_int("nfull", nfull);
  f->dump_int("nremote", nremote);
  f->dump_int("nnull", nnull);

  f->open_array_section("full bits");
  for (list<ceph::shared_ptr<fullbit> >::const_iterator
      iter = dfull.begin(); iter != dfull.end(); ++iter) {
    f->open_object_section("fullbit");
    (*iter)->dump(f);
    f->close_section(); // fullbit
  }
  f->close_section(); // full bits
  f->open_array_section("remote bits");
  for (list<remotebit>::const_iterator
      iter = dremote.begin(); iter != dremote.end(); ++iter) {
    f->open_object_section("remotebit");
    (*iter).dump(f);
    f->close_section(); // remotebit
  }
  f->close_section(); // remote bits
  f->open_array_section("null bits");
  for (list<nullbit>::const_iterator
      iter = dnull.begin(); iter != dnull.end(); ++iter) {
    f->open_object_section("null bit");
    (*iter).dump(f);
    f->close_section(); // null bit
  }
  f->close_section(); // null bits
}

void EMetaBlob::dirlump::generate_test_instances(list<dirlump*>& ls)
{
  ls.push_back(new dirlump());
}

/**
 * EMetaBlob proper
 */
void EMetaBlob::encode(bufferlist& bl) const
{
  ENCODE_START(7, 5, bl);
  ::encode(lump_order, bl);
  ::encode(lump_map, bl);
  ::encode(roots, bl);
  ::encode(table_tids, bl);
  ::encode(opened_ino, bl);
  ::encode(allocated_ino, bl);
  ::encode(used_preallocated_ino, bl);
  ::encode(preallocated_inos, bl);
  ::encode(client_name, bl);
  ::encode(inotablev, bl);
  ::encode(sessionmapv, bl);
  ::encode(truncate_start, bl);
  ::encode(truncate_finish, bl);
  ::encode(destroyed_inodes, bl);
  ::encode(client_reqs, bl);
  ::encode(renamed_dirino, bl);
  ::encode(renamed_dir_frags, bl);
  {
    // make MDS use v6 format happy
    int64_t i = -1;
    bool b = false;
    ::encode(i, bl);
    ::encode(b, bl);
  }
  ENCODE_FINISH(bl);
}
void EMetaBlob::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
  ::decode(lump_order, bl);
  ::decode(lump_map, bl);
  if (struct_v >= 4) {
    ::decode(roots, bl);
  } else {
    bufferlist rootbl;
    ::decode(rootbl, bl);
    if (rootbl.length()) {
      bufferlist::iterator p = rootbl.begin();
      roots.push_back(ceph::shared_ptr<fullbit>(new fullbit(p)));
    }
  }
  ::decode(table_tids, bl);
  ::decode(opened_ino, bl);
  ::decode(allocated_ino, bl);
  ::decode(used_preallocated_ino, bl);
  ::decode(preallocated_inos, bl);
  ::decode(client_name, bl);
  ::decode(inotablev, bl);
  ::decode(sessionmapv, bl);
  ::decode(truncate_start, bl);
  ::decode(truncate_finish, bl);
  ::decode(destroyed_inodes, bl);
  if (struct_v >= 2) {
    ::decode(client_reqs, bl);
  } else {
    list<metareqid_t> r;
    ::decode(r, bl);
    while (!r.empty()) {
	client_reqs.push_back(pair<metareqid_t,uint64_t>(r.front(), 0));
	r.pop_front();
    }
  }
  if (struct_v >= 3) {
    ::decode(renamed_dirino, bl);
    ::decode(renamed_dir_frags, bl);
  }
  if (struct_v >= 6) {
    // ignore
    int64_t i;
    bool b;
    ::decode(i, bl);
    ::decode(b, bl);
  }
  DECODE_FINISH(bl);
}

void EMetaBlob::dump(Formatter *f) const
{
  f->open_array_section("lumps");
  for (list<dirfrag_t>::const_iterator i = lump_order.begin();
       i != lump_order.end(); ++i) {
    f->open_object_section("lump");
    f->open_object_section("dirfrag");
    f->dump_stream("dirfrag") << *i;
    f->close_section(); // dirfrag
    f->open_object_section("dirlump");
    lump_map.at(*i).dump(f);
    f->close_section(); // dirlump
    f->close_section(); // lump
  }
  f->close_section(); // lumps
  
  f->open_array_section("roots");
  for (list<ceph::shared_ptr<fullbit> >::const_iterator i = roots.begin();
       i != roots.end(); ++i) {
    f->open_object_section("root");
    (*i)->dump(f);
    f->close_section(); // root
  }
  f->close_section(); // roots

  f->open_array_section("tableclient tranactions");
  for (list<pair<__u8,version_t> >::const_iterator i = table_tids.begin();
       i != table_tids.end(); ++i) {
    f->open_object_section("transaction");
    f->dump_int("tid", i->first);
    f->dump_int("version", i->second);
    f->close_section(); // transaction
  }
  f->close_section(); // tableclient transactions
  
  f->dump_int("renamed directory inodeno", renamed_dirino);
  
  f->open_array_section("renamed directory fragments");
  for (list<frag_t>::const_iterator i = renamed_dir_frags.begin();
       i != renamed_dir_frags.end(); ++i) {
    f->dump_int("frag", *i);
  }
  f->close_section(); // renamed directory fragments

  f->dump_int("inotable version", inotablev);
  f->dump_int("SessionMap version", sessionmapv);
  f->dump_int("allocated ino", allocated_ino);
  
  f->dump_stream("preallocated inos") << preallocated_inos;
  f->dump_int("used preallocated ino", used_preallocated_ino);

  f->open_object_section("client name");
  client_name.dump(f);
  f->close_section(); // client name

  f->open_array_section("inodes starting a truncate");
  for(list<inodeno_t>::const_iterator i = truncate_start.begin();
      i != truncate_start.end(); ++i) {
    f->dump_int("inodeno", *i);
  }
  f->close_section(); // truncate inodes
  f->open_array_section("inodes finishing a truncated");
  for(map<inodeno_t,uint64_t>::const_iterator i = truncate_finish.begin();
      i != truncate_finish.end(); ++i) {
    f->open_object_section("inode+segment");
    f->dump_int("inodeno", i->first);
    f->dump_int("truncate starting segment", i->second);
    f->close_section(); // truncated inode
  }
  f->close_section(); // truncate finish inodes

  f->open_array_section("destroyed inodes");
  for(vector<inodeno_t>::const_iterator i = destroyed_inodes.begin();
      i != destroyed_inodes.end(); ++i) {
    f->dump_int("inodeno", *i);
  }
  f->close_section(); // destroyed inodes

  f->open_array_section("client requests");
  for(list<pair<metareqid_t,uint64_t> >::const_iterator i = client_reqs.begin();
      i != client_reqs.end(); ++i) {
    f->open_object_section("Client request");
    f->dump_stream("request ID") << i->first;
    f->dump_int("oldest request on client", i->second);
    f->close_section(); // request
  }
  f->close_section(); // client requests
}

void EMetaBlob::generate_test_instances(list<EMetaBlob*>& ls)
{
  ls.push_back(new EMetaBlob());
}

// ----

void ESession::encode(bufferlist &bl) const
{
  ENCODE_START(4, 3, bl);
  ::encode(stamp, bl);
  ::encode(client_inst, bl);
  ::encode(open, bl);
  ::encode(cmapv, bl);
  ::encode(inos, bl);
  ::encode(inotablev, bl);
  ::encode(client_metadata, bl);
  ENCODE_FINISH(bl);
}

void ESession::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(client_inst, bl);
  ::decode(open, bl);
  ::decode(cmapv, bl);
  ::decode(inos, bl);
  ::decode(inotablev, bl);
  if (struct_v >= 4) {
    ::decode(client_metadata, bl);
  }
  DECODE_FINISH(bl);
}

void ESession::dump(Formatter *f) const
{
  f->dump_stream("client instance") << client_inst;
  f->dump_string("open", open ? "true" : "false");
  f->dump_int("client map version", cmapv);
  f->dump_stream("inos") << inos;
  f->dump_int("inotable version", inotablev);
  f->open_object_section("client_metadata");
  for (map<string, string>::const_iterator i = client_metadata.begin();
      i != client_metadata.end(); ++i) {
    f->dump_string(i->first.c_str(), i->second);
  }
  f->close_section();  // client_metadata
}

void ESession::generate_test_instances(list<ESession*>& ls)
{
  ls.push_back(new ESession);
}

// -----
void ESessions::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(client_map, bl);
  ::encode(cmapv, bl);
  ::encode(stamp, bl);
  ENCODE_FINISH(bl);
}

void ESessions::decode_old(bufferlist::iterator &bl)
{
  ::decode(client_map, bl);
  ::decode(cmapv, bl);
  if (!bl.end())
    ::decode(stamp, bl);
}

void ESessions::decode_new(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(client_map, bl);
  ::decode(cmapv, bl);
  if (!bl.end())
    ::decode(stamp, bl);
  DECODE_FINISH(bl);
}

void ESessions::dump(Formatter *f) const
{
  f->dump_int("client map version", cmapv);

  f->open_array_section("client map");
  for (map<client_t,entity_inst_t>::const_iterator i = client_map.begin();
       i != client_map.end(); ++i) {
    f->open_object_section("client");
    f->dump_int("client id", i->first.v);
    f->dump_stream("client entity") << i->second;
    f->close_section(); // client
  }
  f->close_section(); // client map
}

void ESessions::generate_test_instances(list<ESessions*>& ls)
{
  ls.push_back(new ESessions());
}

//

void ETableServer::encode(bufferlist& bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(table, bl);
  ::encode(op, bl);
  ::encode(reqid, bl);
  ::encode(bymds, bl);
  ::encode(mutation, bl);
  ::encode(tid, bl);
  ::encode(version, bl);
  ENCODE_FINISH(bl);
}

void ETableServer::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(table, bl);
  ::decode(op, bl);
  ::decode(reqid, bl);
  ::decode(bymds, bl);
  ::decode(mutation, bl);
  ::decode(tid, bl);
  ::decode(version, bl);
  DECODE_FINISH(bl);
}

void ETableServer::dump(Formatter *f) const
{
  f->dump_int("table id", table);
  f->dump_int("op", op);
  f->dump_int("request id", reqid);
  f->dump_int("by mds", bymds);
  f->dump_int("tid", tid);
  f->dump_int("version", version);
}

void ETableServer::generate_test_instances(list<ETableServer*>& ls)
{
  ls.push_back(new ETableServer());
}

//

void ETableClient::encode(bufferlist& bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(table, bl);
  ::encode(op, bl);
  ::encode(tid, bl);
  ENCODE_FINISH(bl);
}

void ETableClient::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(table, bl);
  ::decode(op, bl);
  ::decode(tid, bl);
  DECODE_FINISH(bl);
}

void ETableClient::dump(Formatter *f) const
{
  f->dump_int("table", table);
  f->dump_int("op", op);
  f->dump_int("tid", tid);
}

void ETableClient::generate_test_instances(list<ETableClient*>& ls)
{
  ls.push_back(new ETableClient());
}

//

void EUpdate::encode(bufferlist &bl) const
{
  ENCODE_START(4, 4, bl);
  ::encode(stamp, bl);
  ::encode(type, bl);
  ::encode(metablob, bl);
  ::encode(client_map, bl);
  ::encode(cmapv, bl);
  ::encode(reqid, bl);
  ::encode(had_slaves, bl);
  ENCODE_FINISH(bl);
}
 
void EUpdate::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(type, bl);
  ::decode(metablob, bl);
  ::decode(client_map, bl);
  if (struct_v >= 3)
    ::decode(cmapv, bl);
  ::decode(reqid, bl);
  ::decode(had_slaves, bl);
  DECODE_FINISH(bl);
}

void EUpdate::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob

  f->dump_string("type", type);
  f->dump_int("client map length", client_map.length());
  f->dump_int("client map version", cmapv);
  f->dump_stream("reqid") << reqid;
  f->dump_string("had slaves", had_slaves ? "true" : "false");
}

void EUpdate::generate_test_instances(list<EUpdate*>& ls)
{
  ls.push_back(new EUpdate());
}

//

void EOpen::encode(bufferlist &bl) const {
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(metablob, bl);
  ::encode(inos, bl);
  ENCODE_FINISH(bl);
} 

void EOpen::decode(bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(metablob, bl);
  ::decode(inos, bl);
  DECODE_FINISH(bl);
}

void EOpen::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob
  f->open_array_section("inos involved");
  for (vector<inodeno_t>::const_iterator i = inos.begin();
       i != inos.end(); ++i) {
    f->dump_int("ino", *i);
  }
  f->close_section(); // inos
}

void EOpen::generate_test_instances(list<EOpen*>& ls)
{
  ls.push_back(new EOpen());
  ls.push_back(new EOpen());
  ls.back()->add_ino(0);
}

//

void ECommitted::encode(bufferlist& bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(reqid, bl);
  ENCODE_FINISH(bl);
} 

void ECommitted::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(reqid, bl);
  DECODE_FINISH(bl);
}

void ECommitted::dump(Formatter *f) const {
  f->dump_stream("stamp") << stamp;
  f->dump_stream("reqid") << reqid;
}

void ECommitted::generate_test_instances(list<ECommitted*>& ls)
{
  ls.push_back(new ECommitted);
  ls.push_back(new ECommitted);
  ls.back()->stamp = utime_t(1, 2);
  ls.back()->reqid = metareqid_t(entity_name_t::CLIENT(123), 456);
}


//

void link_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(reqid, bl);
  ::encode(ino, bl);
  ::encode(was_inc, bl);
  ::encode(old_ctime, bl);
  ::encode(old_dir_mtime, bl);
  ::encode(old_dir_rctime, bl);
  ENCODE_FINISH(bl);
}

void link_rollback::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(reqid, bl);
  ::decode(ino, bl);
  ::decode(was_inc, bl);
  ::decode(old_ctime, bl);
  ::decode(old_dir_mtime, bl);
  ::decode(old_dir_rctime, bl);
  DECODE_FINISH(bl);
}

void link_rollback::dump(Formatter *f) const
{
  f->dump_stream("metareqid") << reqid;
  f->dump_int("ino", ino);
  f->dump_string("was incremented", was_inc ? "true" : "false");
  f->dump_stream("old_ctime") << old_ctime;
  f->dump_stream("old_dir_mtime") << old_dir_mtime;
  f->dump_stream("old_dir_rctime") << old_dir_rctime;
}

void link_rollback::generate_test_instances(list<link_rollback*>& ls)
{
  ls.push_back(new link_rollback());
}

void rmdir_rollback::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(reqid, bl);
  ::encode(src_dir, bl);
  ::encode(src_dname, bl);
  ::encode(dest_dir, bl);
  ::encode(dest_dname, bl);
  ENCODE_FINISH(bl);
}

void rmdir_rollback::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(reqid, bl);
  ::decode(src_dir, bl);
  ::decode(src_dname, bl);
  ::decode(dest_dir, bl);
  ::decode(dest_dname, bl);
  DECODE_FINISH(bl);
}

void rmdir_rollback::dump(Formatter *f) const
{
  f->dump_stream("metareqid") << reqid;
  f->dump_stream("source directory") << src_dir;
  f->dump_string("source dname", src_dname);
  f->dump_stream("destination directory") << dest_dir;
  f->dump_string("destination dname", dest_dname);
}

void rmdir_rollback::generate_test_instances(list<rmdir_rollback*>& ls)
{
  ls.push_back(new rmdir_rollback());
}

void rename_rollback::drec::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(dirfrag, bl);
  ::encode(dirfrag_old_mtime, bl);
  ::encode(dirfrag_old_rctime, bl);
  ::encode(ino, bl);
  ::encode(remote_ino, bl);
  ::encode(dname, bl);
  ::encode(remote_d_type, bl);
  ::encode(old_ctime, bl);
  ENCODE_FINISH(bl);
}

void rename_rollback::drec::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(dirfrag, bl);
  ::decode(dirfrag_old_mtime, bl);
  ::decode(dirfrag_old_rctime, bl);
  ::decode(ino, bl);
  ::decode(remote_ino, bl);
  ::decode(dname, bl);
  ::decode(remote_d_type, bl);
  ::decode(old_ctime, bl);
  DECODE_FINISH(bl);
}

void rename_rollback::drec::dump(Formatter *f) const
{
  f->dump_stream("directory fragment") << dirfrag;
  f->dump_stream("directory old mtime") << dirfrag_old_mtime;
  f->dump_stream("directory old rctime") << dirfrag_old_rctime;
  f->dump_int("ino", ino);
  f->dump_int("remote ino", remote_ino);
  f->dump_string("dname", dname);
  uint32_t type = DTTOIF(remote_d_type) & S_IFMT; // convert to type entries
  string type_string;
  switch(type) {
  case S_IFREG:
    type_string = "file"; break;
  case S_IFLNK:
    type_string = "symlink"; break;
  case S_IFDIR:
    type_string = "directory"; break;
  default:
    type_string = "UNKNOWN-" + stringify((int)type); break;
  }
  f->dump_string("remote dtype", type_string);
  f->dump_stream("old ctime") << old_ctime;
}

void rename_rollback::drec::generate_test_instances(list<drec*>& ls)
{
  ls.push_back(new drec());
  ls.back()->remote_d_type = IFTODT(S_IFREG);
}

void rename_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(reqid, bl);
  encode(orig_src, bl);
  encode(orig_dest, bl);
  encode(stray, bl);
  ::encode(ctime, bl);
  ENCODE_FINISH(bl);
}

void rename_rollback::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(reqid, bl);
  decode(orig_src, bl);
  decode(orig_dest, bl);
  decode(stray, bl);
  ::decode(ctime, bl);
  DECODE_FINISH(bl);
}

void rename_rollback::dump(Formatter *f) const
{
  f->dump_stream("request id") << reqid;
  f->open_object_section("original src drec");
  orig_src.dump(f);
  f->close_section(); // original src drec
  f->open_object_section("original dest drec");
  orig_dest.dump(f);
  f->close_section(); // original dest drec
  f->open_object_section("stray drec");
  stray.dump(f);
  f->close_section(); // stray drec
  f->dump_stream("ctime") << ctime;
}

void rename_rollback::generate_test_instances(list<rename_rollback*>& ls)
{
  ls.push_back(new rename_rollback());
  ls.back()->orig_src.remote_d_type = IFTODT(S_IFREG);
  ls.back()->orig_dest.remote_d_type = IFTODT(S_IFREG);
  ls.back()->stray.remote_d_type = IFTODT(S_IFREG);
}

void ESlaveUpdate::encode(bufferlist &bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(type, bl);
  ::encode(reqid, bl);
  ::encode(master, bl);
  ::encode(op, bl);
  ::encode(origop, bl);
  ::encode(commit, bl);
  ::encode(rollback, bl);
  ENCODE_FINISH(bl);
} 

void ESlaveUpdate::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(type, bl);
  ::decode(reqid, bl);
  ::decode(master, bl);
  ::decode(op, bl);
  ::decode(origop, bl);
  ::decode(commit, bl);
  ::decode(rollback, bl);
  DECODE_FINISH(bl);
}

void ESlaveUpdate::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  commit.dump(f);
  f->close_section(); // metablob

  f->dump_int("rollback length", rollback.length());
  f->dump_string("type", type);
  f->dump_stream("metareqid") << reqid;
  f->dump_int("master", master);
  f->dump_int("op", op);
  f->dump_int("original op", origop);
}

void ESlaveUpdate::generate_test_instances(list<ESlaveUpdate*>& ls)
{
  ls.push_back(new ESlaveUpdate());
}


//

void ESubtreeMap::encode(bufferlist& bl) const
{
  ENCODE_START(6, 5, bl);
  ::encode(stamp, bl);
  ::encode(metablob, bl);
  ::encode(subtrees, bl);
  ::encode(ambiguous_subtrees, bl);
  ::encode(expire_pos, bl);
  ::encode(event_seq, bl);
  ENCODE_FINISH(bl);
}
 
void ESubtreeMap::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 5, 5, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(metablob, bl);
  ::decode(subtrees, bl);
  if (struct_v >= 4)
    ::decode(ambiguous_subtrees, bl);
  if (struct_v >= 3)
    ::decode(expire_pos, bl);
  if (struct_v >= 6)
    ::decode(event_seq, bl);
  DECODE_FINISH(bl);
}

void ESubtreeMap::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob
  
  f->open_array_section("subtrees");
  for(map<dirfrag_t,vector<dirfrag_t> >::const_iterator i = subtrees.begin();
      i != subtrees.end(); ++i) {
    f->open_object_section("tree");
    f->dump_stream("root dirfrag") << i->first;
    for (vector<dirfrag_t>::const_iterator j = i->second.begin();
	 j != i->second.end(); ++j) {
      f->dump_stream("bound dirfrag") << *j;
    }
    f->close_section(); // tree
  }
  f->close_section(); // subtrees

  f->open_array_section("ambiguous subtrees");
  for(set<dirfrag_t>::const_iterator i = ambiguous_subtrees.begin();
      i != ambiguous_subtrees.end(); ++i) {
    f->dump_stream("dirfrag") << *i;
  }
  f->close_section(); // ambiguous subtrees

  f->dump_int("expire position", expire_pos);
}

void ESubtreeMap::generate_test_instances(list<ESubtreeMap*>& ls)
{
  ls.push_back(new ESubtreeMap());
}


//

void EFragment::encode(bufferlist &bl) const {
  ENCODE_START(5, 4, bl);
  ::encode(stamp, bl);
  ::encode(op, bl);
  ::encode(ino, bl);
  ::encode(basefrag, bl);
  ::encode(bits, bl);
  ::encode(metablob, bl);
  ::encode(orig_frags, bl);
  ::encode(rollback, bl);
  ENCODE_FINISH(bl);
}

void EFragment::decode(bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  if (struct_v >= 3)
    ::decode(op, bl);
  else
    op = OP_ONESHOT;
  ::decode(ino, bl);
  ::decode(basefrag, bl);
  ::decode(bits, bl);
  ::decode(metablob, bl);
  if (struct_v >= 5) {
    ::decode(orig_frags, bl);
    ::decode(rollback, bl);
  }
  DECODE_FINISH(bl);
}

void EFragment::dump(Formatter *f) const
{
  /*f->open_object_section("Metablob");
  metablob.dump(f); // sadly we don't have this; dunno if we'll get it
  f->close_section();*/
  f->dump_string("op", op_name(op));
  f->dump_stream("ino") << ino;
  f->dump_stream("base frag") << basefrag;
  f->dump_int("bits", bits);
}

void EFragment::generate_test_instances(list<EFragment*>& ls)
{
  ls.push_back(new EFragment);
  ls.push_back(new EFragment);
  ls.back()->op = OP_PREPARE;
  ls.back()->ino = 1;
  ls.back()->bits = 5;
}

void dirfrag_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(fnode, bl);
  ENCODE_FINISH(bl);
}

void dirfrag_rollback::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(fnode, bl);
  DECODE_FINISH(bl);
}

//

void EExport::encode(bufferlist& bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(metablob, bl);
  ::encode(base, bl);
  ::encode(bounds, bl);
  ENCODE_FINISH(bl);
}

void EExport::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(metablob, bl);
  ::decode(base, bl);
  ::decode(bounds, bl);
  DECODE_FINISH(bl);
}

void EExport::dump(Formatter *f) const
{
  f->dump_float("stamp", (double)stamp);
  /*f->open_object_section("Metablob");
  metablob.dump(f); // sadly we don't have this; dunno if we'll get it
  f->close_section();*/
  f->dump_stream("base dirfrag") << base;
  f->open_array_section("bounds dirfrags");
  for (set<dirfrag_t>::const_iterator i = bounds.begin();
      i != bounds.end(); ++i) {
    f->dump_stream("dirfrag") << *i;
  }
  f->close_section(); // bounds dirfrags
}

void EExport::generate_test_instances(list<EExport*>& ls)
{
  EExport *sample = new EExport();
  ls.push_back(sample);
}


//

void EImportStart::encode(bufferlist &bl) const {
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(base, bl);
  ::encode(metablob, bl);
  ::encode(bounds, bl);
  ::encode(cmapv, bl);
  ::encode(client_map, bl);
  ENCODE_FINISH(bl);
}

void EImportStart::decode(bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(base, bl);
  ::decode(metablob, bl);
  ::decode(bounds, bl);
  ::decode(cmapv, bl);
  ::decode(client_map, bl);
  DECODE_FINISH(bl);
}

void EImportStart::dump(Formatter *f) const
{
  f->dump_stream("base dirfrag") << base;
  f->open_array_section("boundary dirfrags");
  for (vector<dirfrag_t>::const_iterator iter = bounds.begin();
      iter != bounds.end(); ++iter) {
    f->dump_stream("frag") << *iter;
  }
  f->close_section();
}

void EImportStart::generate_test_instances(list<EImportStart*>& ls)
{
  ls.push_back(new EImportStart);
}


//

void EImportFinish::encode(bufferlist& bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(base, bl);
  ::encode(success, bl);
  ENCODE_FINISH(bl);
}

void EImportFinish::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(base, bl);
  ::decode(success, bl);
  DECODE_FINISH(bl);
}

void EImportFinish::dump(Formatter *f) const
{
  f->dump_stream("base dirfrag") << base;
  f->dump_string("success", success ? "true" : "false");
}
void EImportFinish::generate_test_instances(list<EImportFinish*>& ls)
{
  ls.push_back(new EImportFinish);
  ls.push_back(new EImportFinish);
  ls.back()->success = true;
}


///

void EResetJournal::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(stamp, bl);
  ENCODE_FINISH(bl);
}
 
void EResetJournal::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(stamp, bl);
  DECODE_FINISH(bl);
}

void EResetJournal::dump(Formatter *f) const
{
  f->dump_stream("timestamp") << stamp;
}

void EResetJournal::generate_test_instances(list<EResetJournal*>& ls)
{
  ls.push_back(new EResetJournal());
}


///

void ENoOp::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(pad_size, bl);
  uint8_t const pad = 0xff;
  for (unsigned int i = 0; i < pad_size; ++i) {
    ::encode(pad, bl);
  }
  ENCODE_FINISH(bl);
}


void ENoOp::decode(bufferlist::iterator &bl)
{
  DECODE_START(2, bl);
  ::decode(pad_size, bl);
  if (bl.get_remaining() != pad_size) {
    // This is spiritually an assertion, but expressing in a way that will let
    // journal debug tools catch it and recognise a malformed entry.
    throw buffer::end_of_buffer();
  } else {
    bl.advance(pad_size);
  }
  DECODE_FINISH(bl);
}
