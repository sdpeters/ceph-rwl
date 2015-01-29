// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "common/config.h"
#include "osdc/Journaler.h"
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

#include "include/stringify.h"

#include "LogSegment.h"

#include "MDS.h"
#include "MDLog.h"
#include "MDCache.h"
#include "Server.h"
#include "Migrator.h"
#include "Mutation.h"

#include "InoTable.h"
#include "MDSTableClient.h"
#include "MDSTableServer.h"

#include "Locker.h"

#define dout_subsys ceph_subsys_mds
#undef DOUT_COND
#define DOUT_COND(cct, l) (l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_log \
			      || l <= cct->_conf->debug_mds_log_expire)
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".journal "


// -----------------------
// LogSegment

void LogSegment::try_to_expire(MDS *mds, MDSGatherBuilder &gather_bld, int op_prio)
{
  set<CDir*> commit;

  dout(6) << "LogSegment(" << seq << "/" << offset << ").try_to_expire" << dendl;

  assert(g_conf->mds_kill_journal_expire_at != 1);

  // commit dirs
  for (elist<CDir*>::iterator p = new_dirfrags.begin(); !p.end(); ++p) {
    dout(20) << " new_dirfrag " << **p << dendl;
    assert((*p)->is_auth());
    commit.insert(*p);
  }
  for (elist<CDir*>::iterator p = dirty_dirfrags.begin(); !p.end(); ++p) {
    dout(20) << " dirty_dirfrag " << **p << dendl;
    assert((*p)->is_auth());
    commit.insert(*p);
  }
  for (elist<CDentry*>::iterator p = dirty_dentries.begin(); !p.end(); ++p) {
    dout(20) << " dirty_dentry " << **p << dendl;
    assert((*p)->is_auth());
    commit.insert((*p)->get_dir());
  }
  for (elist<CInode*>::iterator p = dirty_inodes.begin(); !p.end(); ++p) {
    dout(20) << " dirty_inode " << **p << dendl;
    assert((*p)->is_auth());
    if ((*p)->is_base()) {
      (*p)->store(gather_bld.new_sub());
    } else
      commit.insert((*p)->get_parent_dn()->get_dir());
  }

  if (!commit.empty()) {
    for (set<CDir*>::iterator p = commit.begin();
	 p != commit.end();
	 ++p) {
      CDir *dir = *p;
      assert(dir->is_auth());
      if (dir->can_auth_pin()) {
	dout(15) << "try_to_expire committing " << *dir << dendl;
	dir->commit(0, gather_bld.new_sub(), false, op_prio);
      } else {
	dout(15) << "try_to_expire waiting for unfreeze on " << *dir << dendl;
	dir->add_waiter(CDir::WAIT_UNFREEZE, gather_bld.new_sub());
      }
    }
  }

  // master ops with possibly uncommitted slaves
  for (set<metareqid_t>::iterator p = uncommitted_masters.begin();
       p != uncommitted_masters.end();
       ++p) {
    dout(10) << "try_to_expire waiting for slaves to ack commit on " << *p << dendl;
    mds->mdcache->wait_for_uncommitted_master(*p, gather_bld.new_sub());
  }

  // uncommitted fragments
  for (set<dirfrag_t>::iterator p = uncommitted_fragments.begin();
       p != uncommitted_fragments.end();
       ++p) {
    dout(10) << "try_to_expire waiting for uncommitted fragment " << *p << dendl;
    mds->mdcache->wait_for_uncommitted_fragment(*p, gather_bld.new_sub());
  }

  // nudge scatterlocks
  for (elist<CInode*>::iterator p = dirty_dirfrag_dir.begin(); !p.end(); ++p) {
    CInode *in = *p;
    dout(10) << "try_to_expire waiting for dirlock flush on " << *in << dendl;
    mds->locker->scatter_nudge(&in->filelock, gather_bld.new_sub());
  }
  for (elist<CInode*>::iterator p = dirty_dirfrag_dirfragtree.begin(); !p.end(); ++p) {
    CInode *in = *p;
    dout(10) << "try_to_expire waiting for dirfragtreelock flush on " << *in << dendl;
    mds->locker->scatter_nudge(&in->dirfragtreelock, gather_bld.new_sub());
  }
  for (elist<CInode*>::iterator p = dirty_dirfrag_nest.begin(); !p.end(); ++p) {
    CInode *in = *p;
    dout(10) << "try_to_expire waiting for nest flush on " << *in << dendl;
    mds->locker->scatter_nudge(&in->nestlock, gather_bld.new_sub());
  }

  assert(g_conf->mds_kill_journal_expire_at != 2);

  // open files
  if (!open_files.empty()) {
    assert(!mds->mdlog->is_capped()); // hmm FIXME
    EOpen *le = 0;
    LogSegment *ls = mds->mdlog->get_current_segment();
    assert(ls != this);
    elist<CInode*>::iterator p = open_files.begin(member_offset(CInode, item_open_file));
    while (!p.end()) {
      CInode *in = *p;
      assert(in->last == CEPH_NOSNAP);
      ++p;
      if (in->is_auth() && !in->is_ambiguous_auth() && in->is_any_caps()) {
	if (in->is_any_caps_wanted()) {
	  dout(20) << "try_to_expire requeueing open file " << *in << dendl;
	  if (!le) {
	    le = new EOpen(mds->mdlog);
	    mds->mdlog->start_entry(le);
	  }
	  le->add_clean_inode(in);
	  ls->open_files.push_back(&in->item_open_file);
	} else {
	  // drop inodes that aren't wanted
	  dout(20) << "try_to_expire not requeueing and delisting unwanted file " << *in << dendl;
	  in->item_open_file.remove_myself();
	}
      } else {
	/*
	 * we can get a capless inode here if we replay an open file, the client fails to
	 * reconnect it, but does REPLAY an open request (that adds it to the logseg).  AFAICS
	 * it's ok for the client to replay an open on a file it doesn't have in it's cache
	 * anymore.
	 *
	 * this makes the mds less sensitive to strict open_file consistency, although it does
	 * make it easier to miss subtle problems.
	 */
	dout(20) << "try_to_expire not requeueing and delisting capless file " << *in << dendl;
	in->item_open_file.remove_myself();
      }
    }
    if (le) {
      mds->mdlog->submit_entry(le, gather_bld.new_sub());
      dout(10) << "try_to_expire waiting for open files to rejournal" << dendl;
    }
  }

  assert(g_conf->mds_kill_journal_expire_at != 3);

  // backtraces to be stored/updated
  for (elist<CInode*>::iterator p = dirty_parent_inodes.begin(); !p.end(); ++p) {
    CInode *in = *p;
    assert(in->is_auth());
    if (in->can_auth_pin()) {
      dout(15) << "try_to_expire waiting for storing backtrace on " << *in << dendl;
      in->store_backtrace(gather_bld.new_sub(), op_prio);
    } else {
      dout(15) << "try_to_expire waiting for unfreeze on " << *in << dendl;
      in->add_waiter(CInode::WAIT_UNFREEZE, gather_bld.new_sub());
    }
  }

  assert(g_conf->mds_kill_journal_expire_at != 4);

  // slave updates
  for (elist<MDSlaveUpdate*>::iterator p = slave_updates.begin(member_offset(MDSlaveUpdate,
									     item));
       !p.end(); ++p) {
    MDSlaveUpdate *su = *p;
    dout(10) << "try_to_expire waiting on slave update " << su << dendl;
    assert(su->waiter == 0);
    su->waiter = gather_bld.new_sub();
  }

  // idalloc
  if (inotablev > mds->inotable->get_committed_version()) {
    dout(10) << "try_to_expire saving inotable table, need " << inotablev
	      << ", committed is " << mds->inotable->get_committed_version()
	      << " (" << mds->inotable->get_committing_version() << ")"
	      << dendl;
    mds->inotable->save(gather_bld.new_sub(), inotablev);
  }

  // sessionmap
  if (sessionmapv > mds->sessionmap.committed) {
    dout(10) << "try_to_expire saving sessionmap, need " << sessionmapv 
	      << ", committed is " << mds->sessionmap.committed
	      << " (" << mds->sessionmap.committing << ")"
	      << dendl;
    mds->sessionmap.save(gather_bld.new_sub(), sessionmapv);
  }

  // pending commit atids
  for (map<int, ceph::unordered_set<version_t> >::iterator p = pending_commit_tids.begin();
       p != pending_commit_tids.end();
       ++p) {
    MDSTableClient *client = mds->get_table_client(p->first);
    assert(client);
    for (ceph::unordered_set<version_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(10) << "try_to_expire " << get_mdstable_name(p->first) << " transaction " << *q 
	       << " pending commit (not yet acked), waiting" << dendl;
      assert(!client->has_committed(*q));
      client->wait_for_ack(*q, gather_bld.new_sub());
    }
  }
  
  // table servers
  for (map<int, version_t>::iterator p = tablev.begin();
       p != tablev.end();
       ++p) {
    MDSTableServer *server = mds->get_table_server(p->first);
    assert(server);
    if (p->second > server->get_committed_version()) {
      dout(10) << "try_to_expire waiting for " << get_mdstable_name(p->first) 
	       << " to save, need " << p->second << dendl;
      server->save(gather_bld.new_sub());
    }
  }

  // truncating
  for (set<CInode*>::iterator p = truncating_inodes.begin();
       p != truncating_inodes.end();
       ++p) {
    dout(10) << "try_to_expire waiting for truncate of " << **p << dendl;
    (*p)->add_waiter(CInode::WAIT_TRUNC, gather_bld.new_sub());
  }
  
  if (gather_bld.has_subs()) {
    dout(6) << "LogSegment(" << seq << "/" << offset << ").try_to_expire waiting" << dendl;
    mds->mdlog->flush();
  } else {
    assert(g_conf->mds_kill_journal_expire_at != 5);
    dout(6) << "LogSegment(" << seq << "/" << offset << ").try_to_expire success" << dendl;
  }
}

#undef DOUT_COND
#define DOUT_COND(cct, l) (l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_log)


// -----------------------
// EMetaBlob

void EMetaBlob::add_dir_context(CDir *dir, int mode)
{
  MDS *mds = dir->cache->mds;

  list<CDentry*> parents;

  // it may be okay not to include the maybe items, if
  //  - we journaled the maybe child inode in this segment
  //  - that subtree turns out to be unambiguously auth
  list<CDentry*> maybe;
  bool maybenot = false;

  while (true) {
    // already have this dir?  (we must always add in order)
    if (lump_map.count(dir->dirfrag())) {
      dout(20) << "EMetaBlob::add_dir_context(" << dir << ") have lump " << dir->dirfrag() << dendl;
      break;
    }

    // stop at root/stray
    CInode *diri = dir->get_inode();
    CDentry *parent = diri->get_projected_parent_dn();

    if (mode == TO_AUTH_SUBTREE_ROOT) {
      // subtree root?
      if (dir->is_subtree_root() && !dir->state_test(CDir::STATE_EXPORTBOUND)) {
	if (dir->is_auth() && !dir->is_ambiguous_auth()) {
	  // it's an auth subtree, we don't need maybe (if any), and we're done.
	  dout(20) << "EMetaBlob::add_dir_context(" << dir << ") reached unambig auth subtree, don't need " << maybe
		   << " at " << *dir << dendl;
	  maybe.clear();
	  break;
	} else {
	  dout(20) << "EMetaBlob::add_dir_context(" << dir << ") reached ambig or !auth subtree, need " << maybe
		   << " at " << *dir << dendl;
	  // we need the maybe list after all!
	  parents.splice(parents.begin(), maybe);
	  maybenot = false;
	}
      }
      
      // was the inode journaled in this blob?
      if (event_seq && diri->last_journaled == event_seq) {
	dout(20) << "EMetaBlob::add_dir_context(" << dir << ") already have diri this blob " << *diri << dendl;
	break;
      }

      // have we journaled this inode since the last subtree map?
      if (!maybenot && last_subtree_map && diri->last_journaled >= last_subtree_map) {
	dout(20) << "EMetaBlob::add_dir_context(" << dir << ") already have diri in this segment (" 
		 << diri->last_journaled << " >= " << last_subtree_map << "), setting maybenot flag "
		 << *diri << dendl;
	maybenot = true;
      }
    }

    if (!parent)
      break;

    if (maybenot) {
      dout(25) << "EMetaBlob::add_dir_context(" << dir << ")      maybe " << *parent << dendl;
      maybe.push_front(parent);
    } else {
      dout(25) << "EMetaBlob::add_dir_context(" << dir << ") definitely " << *parent << dendl;
      parents.push_front(parent);
    }
    
    dir = parent->get_dir();
  }
  
  parents.splice(parents.begin(), maybe);

  dout(20) << "EMetaBlob::add_dir_context final: " << parents << dendl;
  for (list<CDentry*>::iterator p = parents.begin(); p != parents.end(); ++p) {
    assert((*p)->get_projected_linkage()->is_primary());
    add_dentry(*p, false);
  }
}

void EMetaBlob::update_segment(LogSegment *ls)
{
  // dirty inode mtimes
  // -> handled directly by Server.cc, replay()

  // alloc table update?
  if (inotablev)
    ls->inotablev = inotablev;
  if (sessionmapv)
    ls->sessionmapv = sessionmapv;

  // truncated inodes
  // -> handled directly by Server.cc

  // client requests
  //  note the newest request per client
  //if (!client_reqs.empty())
    //    ls->last_client_tid[client_reqs.rbegin()->client] = client_reqs.rbegin()->tid);
}

void EMetaBlob::fullbit::update_inode(MDS *mds, CInode *in)
{
  in->inode = inode;
  in->xattrs = xattrs;
  if (in->inode.is_dir()) {
    if (!(in->dirfragtree == dirfragtree)) {
      dout(10) << "EMetaBlob::fullbit::update_inode dft " << in->dirfragtree << " -> "
	       << dirfragtree << " on " << *in << dendl;
      in->dirfragtree = dirfragtree;
      in->force_dirfrags();
      if (in->has_dirfrags() && in->authority() == CDIR_AUTH_UNDEF) {
	list<CDir*> ls;
	in->get_nested_dirfrags(ls);
	for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
	  CDir *dir = *p;
	  if (dir->get_num_any() == 0 &&
	      mds->mdcache->can_trim_non_auth_dirfrag(dir)) {
	    dout(10) << " closing empty non-auth dirfrag " << *dir << dendl;
	    in->close_dirfrag(dir->get_frag());
	  }
	}
      }
    }

    /*
     * we can do this before linking hte inode bc the split_at would
     * be a no-op.. we have no children (namely open snaprealms) to
     * divy up 
     */
    in->decode_snap_blob(snapbl);  
  } else if (in->inode.is_symlink()) {
    in->symlink = symlink;
  }
  in->old_inodes = old_inodes;
}


/**
 * Get all inodes touched by this metablob.  Includes the 'bits' within
 * dirlumps, and the inodes of the dirs themselves.
 */
void EMetaBlob::get_inodes(
    std::set<inodeno_t> &inodes) const
{
  // For all dirlumps in this metablob
  for (std::map<dirfrag_t, dirlump>::const_iterator i = lump_map.begin(); i != lump_map.end(); ++i) {
    // Record inode of dirlump
    inodeno_t const dir_ino = i->first.ino;
    inodes.insert(dir_ino);

    // Decode dirlump bits
    dirlump const &dl = i->second;
    dl._decode_bits();

    // Record inodes of fullbits
    list<ceph::shared_ptr<fullbit> > const &fb_list = dl.get_dfull();
    for (list<ceph::shared_ptr<fullbit> >::const_iterator
        iter = fb_list.begin(); iter != fb_list.end(); ++iter) {
      inodes.insert((*iter)->inode.ino);
    }

    // Record inodes of remotebits
    list<remotebit> const &rb_list = dl.get_dremote();
    for (list<remotebit>::const_iterator
	iter = rb_list.begin(); iter != rb_list.end(); ++iter) {
      inodes.insert(iter->ino);
    }
  }
}


/**
 * Get a map of dirfrag to set of dentries in that dirfrag which are
 * touched in this operation.
 */
void EMetaBlob::get_dentries(std::map<dirfrag_t, std::set<std::string> > &dentries) const
{
  for (std::map<dirfrag_t, dirlump>::const_iterator i = lump_map.begin(); i != lump_map.end(); ++i) {
    dirlump const &dl = i->second;
    dirfrag_t const &df = i->first;

    // Get all bits
    dl._decode_bits();
    list<ceph::shared_ptr<fullbit> > const &fb_list = dl.get_dfull();
    list<nullbit> const &nb_list = dl.get_dnull();
    list<remotebit> const &rb_list = dl.get_dremote();

    // For all bits, store dentry
    for (list<ceph::shared_ptr<fullbit> >::const_iterator
        iter = fb_list.begin(); iter != fb_list.end(); ++iter) {
      dentries[df].insert((*iter)->dn);

    }
    for (list<nullbit>::const_iterator
	iter = nb_list.begin(); iter != nb_list.end(); ++iter) {
      dentries[df].insert(iter->dn);
    }
    for (list<remotebit>::const_iterator
	iter = rb_list.begin(); iter != rb_list.end(); ++iter) {
      dentries[df].insert(iter->dn);
    }
  }
}



/**
 * Calculate all paths that we can infer are touched by this metablob.  Only uses
 * information local to this metablob so it may only be the path within the
 * subtree.
 */
void EMetaBlob::get_paths(
    std::vector<std::string> &paths) const
{
  // Each dentry has a 'location' which is a 2-tuple of parent inode and dentry name
  typedef std::pair<inodeno_t, std::string> Location;

  // Whenever we see a dentry within a dirlump, we remember it as a child of
  // the dirlump's inode
  std::map<inodeno_t, std::list<std::string> > children;

  // Whenever we see a location for an inode, remember it: this allows us to
  // build a path given an inode
  std::map<inodeno_t, Location> ino_locations;

  // Special case: operations on root inode populate roots but not dirlumps
  if (lump_map.empty() && !roots.empty()) {
    paths.push_back("/");
    return;
  }

  // First pass
  // ==========
  // Build a tiny local metadata cache for the path structure in this metablob
  for (std::map<dirfrag_t, dirlump>::const_iterator i = lump_map.begin(); i != lump_map.end(); ++i) {
    inodeno_t const dir_ino = i->first.ino;
    dirlump const &dl = i->second;
    dl._decode_bits();

    list<ceph::shared_ptr<fullbit> > const &fb_list = dl.get_dfull();
    list<nullbit> const &nb_list = dl.get_dnull();
    list<remotebit> const &rb_list = dl.get_dremote();

    for (list<ceph::shared_ptr<fullbit> >::const_iterator
        iter = fb_list.begin(); iter != fb_list.end(); ++iter) {
      std::string const &dentry = (*iter)->dn;
      children[dir_ino].push_back(dentry);
      ino_locations[(*iter)->inode.ino] = Location(dir_ino, dentry);
    }

    for (list<nullbit>::const_iterator
	iter = nb_list.begin(); iter != nb_list.end(); ++iter) {
      std::string const &dentry = iter->dn;
      children[dir_ino].push_back(dentry);
    }

    for (list<remotebit>::const_iterator
	iter = rb_list.begin(); iter != rb_list.end(); ++iter) {
      std::string const &dentry = iter->dn;
      children[dir_ino].push_back(dentry);
    }
  }

  std::vector<Location> leaf_locations;

  // Second pass
  // ===========
  // Output paths for all childless nodes in the metablob
  for (std::map<dirfrag_t, dirlump>::const_iterator i = lump_map.begin(); i != lump_map.end(); ++i) {
    inodeno_t const dir_ino = i->first.ino;
    dirlump const &dl = i->second;
    dl._decode_bits();

    list<ceph::shared_ptr<fullbit> > const &fb_list = dl.get_dfull();
    for (list<ceph::shared_ptr<fullbit> >::const_iterator
        iter = fb_list.begin(); iter != fb_list.end(); ++iter) {
      std::string const &dentry = (*iter)->dn;
      children[dir_ino].push_back(dentry);
      ino_locations[(*iter)->inode.ino] = Location(dir_ino, dentry);
      if (children.find((*iter)->inode.ino) == children.end()) {
        leaf_locations.push_back(Location(dir_ino, dentry));

      }
    }

    list<nullbit> const &nb_list = dl.get_dnull();
    for (list<nullbit>::const_iterator
	iter = nb_list.begin(); iter != nb_list.end(); ++iter) {
      std::string const &dentry = iter->dn;
      leaf_locations.push_back(Location(dir_ino, dentry));
    }

    list<remotebit> const &rb_list = dl.get_dremote();
    for (list<remotebit>::const_iterator
	iter = rb_list.begin(); iter != rb_list.end(); ++iter) {
      std::string const &dentry = iter->dn;
      leaf_locations.push_back(Location(dir_ino, dentry));
    }
  }

  // For all the leaf locations identified, generate paths
  for (std::vector<Location>::iterator i = leaf_locations.begin(); i != leaf_locations.end(); ++i) {
    Location const &loc = *i;
    std::string path = loc.second;
    inodeno_t ino = loc.first;
    while(ino_locations.find(ino) != ino_locations.end()) {
      Location const &loc = ino_locations[ino];
      if (!path.empty()) {
        path = loc.second + "/" + path;
      } else {
        path = loc.second + path;
      }
      ino = loc.first;
    }

    paths.push_back(path);
  }
}

void EMetaBlob::replay(MDS *mds, LogSegment *logseg, MDSlaveUpdate *slaveup)
{
  dout(10) << "EMetaBlob.replay " << lump_map.size() << " dirlumps by " << client_name << dendl;

  assert(logseg);

  assert(g_conf->mds_kill_journal_replay_at != 1);

  for (list<ceph::shared_ptr<fullbit> >::iterator p = roots.begin(); p != roots.end(); ++p) {
    CInode *in = mds->mdcache->get_inode((*p)->inode.ino);
    bool isnew = in ? false:true;
    if (!in)
      in = new CInode(mds->mdcache, false);
    (*p)->update_inode(mds, in);

    if (isnew)
      mds->mdcache->add_inode(in);
    if ((*p)->is_dirty()) in->_mark_dirty(logseg);
    dout(10) << "EMetaBlob.replay " << (isnew ? " added root ":" updated root ") << *in << dendl;    
  }

  CInode *renamed_diri = 0;
  CDir *olddir = 0;
  if (renamed_dirino) {
    renamed_diri = mds->mdcache->get_inode(renamed_dirino);
    if (renamed_diri)
      dout(10) << "EMetaBlob.replay renamed inode is " << *renamed_diri << dendl;
    else
      dout(10) << "EMetaBlob.replay don't have renamed ino " << renamed_dirino << dendl;

    int nnull = 0;
    for (list<dirfrag_t>::iterator lp = lump_order.begin(); lp != lump_order.end(); ++lp) {
      dirlump &lump = lump_map[*lp];
      if (lump.nnull) {
	dout(10) << "EMetaBlob.replay found null dentry in dir " << *lp << dendl;
	nnull += lump.nnull;
      }
    }
    assert(nnull <= 1);
  }

  // keep track of any inodes we unlink and don't relink elsewhere
  map<CInode*, CDir*> unlinked;
  set<CInode*> linked;

  // walk through my dirs (in order!)
  for (list<dirfrag_t>::iterator lp = lump_order.begin();
       lp != lump_order.end();
       ++lp) {
    dout(10) << "EMetaBlob.replay dir " << *lp << dendl;
    dirlump &lump = lump_map[*lp];

    // the dir 
    CDir *dir = mds->mdcache->get_force_dirfrag(*lp, true);
    if (!dir) {
      // hmm.  do i have the inode?
      CInode *diri = mds->mdcache->get_inode((*lp).ino);
      if (!diri) {
	if (MDS_INO_IS_MDSDIR(lp->ino)) {
	  assert(MDS_INO_MDSDIR(mds->get_nodeid()) != lp->ino);
	  diri = mds->mdcache->create_system_inode(lp->ino, S_IFDIR|0755);
	  diri->state_clear(CInode::STATE_AUTH);
	  dout(10) << "EMetaBlob.replay created base " << *diri << dendl;
	} else {
	  dout(0) << "EMetaBlob.replay missing dir ino  " << (*lp).ino << dendl;
	  assert(0);
	}
      }

      // create the dirfrag
      dir = diri->get_or_open_dirfrag(mds->mdcache, (*lp).frag);

      if (MDS_INO_IS_BASE(lp->ino))
	mds->mdcache->adjust_subtree_auth(dir, CDIR_AUTH_UNDEF);

      dout(10) << "EMetaBlob.replay added dir " << *dir << dendl;  
    }
    dir->set_version( lump.fnode.version );
    dir->fnode = lump.fnode;

    if (lump.is_importing()) {
      dir->state_set(CDir::STATE_AUTH);
      dir->state_clear(CDir::STATE_COMPLETE);
    }
    if (lump.is_dirty()) {
      dir->_mark_dirty(logseg);

      if (!(dir->fnode.rstat == dir->fnode.accounted_rstat)) {
	dout(10) << "EMetaBlob.replay      dirty nestinfo on " << *dir << dendl;
	mds->locker->mark_updated_scatterlock(&dir->inode->nestlock);
	logseg->dirty_dirfrag_nest.push_back(&dir->inode->item_dirty_dirfrag_nest);
      } else {
	dout(10) << "EMetaBlob.replay      clean nestinfo on " << *dir << dendl;
      }
      if (!(dir->fnode.fragstat == dir->fnode.accounted_fragstat)) {
	dout(10) << "EMetaBlob.replay      dirty fragstat on " << *dir << dendl;
	mds->locker->mark_updated_scatterlock(&dir->inode->filelock);
	logseg->dirty_dirfrag_dir.push_back(&dir->inode->item_dirty_dirfrag_dir);
      } else {
	dout(10) << "EMetaBlob.replay      clean fragstat on " << *dir << dendl;
      }
    }
    if (lump.is_dirty_dft()) {
      dout(10) << "EMetaBlob.replay      dirty dirfragtree on " << *dir << dendl;
      dir->state_set(CDir::STATE_DIRTYDFT);
      mds->locker->mark_updated_scatterlock(&dir->inode->dirfragtreelock);
      logseg->dirty_dirfrag_dirfragtree.push_back(&dir->inode->item_dirty_dirfrag_dirfragtree);
    }
    if (lump.is_new())
      dir->mark_new(logseg);
    if (lump.is_complete())
      dir->mark_complete();
    
    dout(10) << "EMetaBlob.replay updated dir " << *dir << dendl;  

    // decode bits
    lump._decode_bits();

    // full dentry+inode pairs
    for (list<ceph::shared_ptr<fullbit> >::const_iterator pp = lump.get_dfull().begin();
	 pp != lump.get_dfull().end();
	 ++pp) {
      ceph::shared_ptr<fullbit> p = *pp;
      CDentry *dn = dir->lookup_exact_snap(p->dn, p->dnlast);
      if (!dn) {
	dn = dir->add_null_dentry(p->dn, p->dnfirst, p->dnlast);
	dn->set_version(p->dnv);
	if (p->is_dirty()) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	dn->set_version(p->dnv);
	if (p->is_dirty()) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay for [" << p->dnfirst << "," << p->dnlast << "] had " << *dn << dendl;
	dn->first = p->dnfirst;
	assert(dn->last == p->dnlast);
      }
      if (lump.is_importing())
	dn->state_set(CDentry::STATE_AUTH);

      CInode *in = mds->mdcache->get_inode(p->inode.ino, p->dnlast);
      if (!in) {
	in = new CInode(mds->mdcache, dn->is_auth(), p->dnfirst, p->dnlast);
	p->update_inode(mds, in);
	mds->mdcache->add_inode(in);
	if (!dn->get_linkage()->is_null()) {
	  if (dn->get_linkage()->is_primary()) {
	    unlinked[dn->get_linkage()->get_inode()] = dir;
	    stringstream ss;
	    ss << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
	       << " " << *dn->get_linkage()->get_inode() << " should be " << p->inode.ino;
	    dout(0) << ss.str() << dendl;
	    mds->clog->warn(ss);
	  }
	  dir->unlink_inode(dn);
	}
	if (unlinked.count(in))
	  linked.insert(in);
	dir->link_primary_inode(dn, in);
	dout(10) << "EMetaBlob.replay added " << *in << dendl;
      } else {
	p->update_inode(mds, in);
	if (dn->get_linkage()->get_inode() != in && in->get_parent_dn()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *in << dendl;
	  unlinked[in] = in->get_parent_dir();
	  in->get_parent_dir()->unlink_inode(in->get_parent_dn());
	}
	if (dn->get_linkage()->get_inode() != in) {
	  if (!dn->get_linkage()->is_null()) { // note: might be remote.  as with stray reintegration.
	    if (dn->get_linkage()->is_primary()) {
	      unlinked[dn->get_linkage()->get_inode()] = dir;
	      stringstream ss;
	      ss << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
		 << " " << *dn->get_linkage()->get_inode() << " should be " << p->inode.ino;
	      dout(0) << ss.str() << dendl;
	      mds->clog->warn(ss);
	    }
	    dir->unlink_inode(dn);
	  }
	  if (unlinked.count(in))
	    linked.insert(in);
	  dir->link_primary_inode(dn, in);
	  dout(10) << "EMetaBlob.replay linked " << *in << dendl;
	} else {
	  dout(10) << "EMetaBlob.replay for [" << p->dnfirst << "," << p->dnlast << "] had " << *in << dendl;
	}
	assert(in->first == p->dnfirst ||
	       (in->is_multiversion() && in->first > p->dnfirst));
      }
      if (p->is_dirty())
	in->_mark_dirty(logseg);
      if (p->is_dirty_parent())
	in->_mark_dirty_parent(logseg, p->is_dirty_pool());
      if (dn->is_auth())
	in->state_set(CInode::STATE_AUTH);
      else
	in->state_clear(CInode::STATE_AUTH);
      assert(g_conf->mds_kill_journal_replay_at != 2);
    }

    // remote dentries
    for (list<remotebit>::const_iterator p = lump.get_dremote().begin();
	 p != lump.get_dremote().end();
	 ++p) {
      CDentry *dn = dir->lookup_exact_snap(p->dn, p->dnlast);
      if (!dn) {
	dn = dir->add_remote_dentry(p->dn, p->ino, p->d_type, p->dnfirst, p->dnlast);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	if (!dn->get_linkage()->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << dendl;
	  if (dn->get_linkage()->is_primary()) {
	    unlinked[dn->get_linkage()->get_inode()] = dir;
	    stringstream ss;
	    ss << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn
	       << " " << *dn->get_linkage()->get_inode() << " should be remote " << p->ino;
	    dout(0) << ss.str() << dendl;
	  }
	  dir->unlink_inode(dn);
	}
	dir->link_remote_inode(dn, p->ino, p->d_type);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay for [" << p->dnfirst << "," << p->dnlast << "] had " << *dn << dendl;
	dn->first = p->dnfirst;
	assert(dn->last == p->dnlast);
      }
      if (lump.is_importing())
	dn->state_set(CDentry::STATE_AUTH);
    }

    // null dentries
    for (list<nullbit>::const_iterator p = lump.get_dnull().begin();
	 p != lump.get_dnull().end();
	 ++p) {
      CDentry *dn = dir->lookup_exact_snap(p->dn, p->dnlast);
      if (!dn) {
	dn = dir->add_null_dentry(p->dn, p->dnfirst, p->dnlast);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	dn->first = p->dnfirst;
	if (!dn->get_linkage()->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << dendl;
	  CInode *in = dn->get_linkage()->get_inode();
	  // For renamed inode, We may call CInode::force_dirfrag() later.
	  // CInode::force_dirfrag() doesn't work well when inode is detached
	  // from the hierarchy.
	  if (!renamed_diri || renamed_diri != in) {
	    if (dn->get_linkage()->is_primary())
	      unlinked[in] = dir;
	    dir->unlink_inode(dn);
	  }
	}
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay had " << *dn << dendl;
	assert(dn->last == p->dnlast);
      }
      olddir = dir;
      if (lump.is_importing())
	dn->state_set(CDentry::STATE_AUTH);
    }
  }

  assert(g_conf->mds_kill_journal_replay_at != 3);

  if (renamed_dirino) {
    if (renamed_diri) {
      assert(unlinked.count(renamed_diri));
      assert(linked.count(renamed_diri));
      olddir = unlinked[renamed_diri];
    } else {
      // we imported a diri we haven't seen before
      renamed_diri = mds->mdcache->get_inode(renamed_dirino);
      assert(renamed_diri);  // it was in the metablob
    }

    if (olddir) {
      if (olddir->authority() != CDIR_AUTH_UNDEF &&
	  renamed_diri->authority() == CDIR_AUTH_UNDEF) {
	assert(slaveup); // auth to non-auth, must be slave prepare
	list<frag_t> leaves;
	renamed_diri->dirfragtree.get_leaves(leaves);
	for (list<frag_t>::iterator p = leaves.begin(); p != leaves.end(); ++p) {
	  CDir *dir = renamed_diri->get_dirfrag(*p);
	  assert(dir);
	  if (dir->get_dir_auth() == CDIR_AUTH_UNDEF)
	    // preserve subtree bound until slave commit
	    slaveup->olddirs.insert(dir->inode);
	  else
	    dir->state_set(CDir::STATE_AUTH);
	}
      }

      mds->mdcache->adjust_subtree_after_rename(renamed_diri, olddir, false);
      
      // see if we can discard the subtree we renamed out of
      CDir *root = mds->mdcache->get_subtree_root(olddir);
      if (root->get_dir_auth() == CDIR_AUTH_UNDEF) {
	if (slaveup) // preserve the old dir until slave commit
	  slaveup->olddirs.insert(olddir->inode);
	else
	  mds->mdcache->try_trim_non_auth_subtree(root);
      }
    }

    // if we are the srci importer, we'll also have some dirfrags we have to open up...
    if (renamed_diri->authority() != CDIR_AUTH_UNDEF) {
      for (list<frag_t>::iterator p = renamed_dir_frags.begin(); p != renamed_dir_frags.end(); ++p) {
	CDir *dir = renamed_diri->get_dirfrag(*p);
	if (dir) {
	  // we already had the inode before, and we already adjusted this subtree accordingly.
	  dout(10) << " already had+adjusted rename import bound " << *dir << dendl;
	  assert(olddir); 
	  continue;
	}
	dir = renamed_diri->get_or_open_dirfrag(mds->mdcache, *p);
	dout(10) << " creating new rename import bound " << *dir << dendl;
	dir->state_clear(CDir::STATE_AUTH);
	mds->mdcache->adjust_subtree_auth(dir, CDIR_AUTH_UNDEF, false);
      }
    }

    // rename may overwrite an empty directory and move it into stray dir.
    unlinked.erase(renamed_diri);
    for (map<CInode*, CDir*>::iterator p = unlinked.begin(); p != unlinked.end(); ++p) {
      if (!linked.count(p->first))
	continue;
      assert(p->first->is_dir());
      mds->mdcache->adjust_subtree_after_rename(p->first, p->second, false);
    }
  }

  if (!unlinked.empty()) {
    for (set<CInode*>::iterator p = linked.begin(); p != linked.end(); ++p)
      unlinked.erase(*p);
    dout(10) << " unlinked set contains " << unlinked << dendl;
    for (map<CInode*, CDir*>::iterator p = unlinked.begin(); p != unlinked.end(); ++p) {
      if (slaveup) // preserve unlinked inodes until slave commit
	slaveup->unlinked.insert(p->first);
      else
	mds->mdcache->remove_inode_recursive(p->first);
    }
  }

  // table client transactions
  for (list<pair<__u8,version_t> >::iterator p = table_tids.begin();
       p != table_tids.end();
       ++p) {
    dout(10) << "EMetaBlob.replay noting " << get_mdstable_name(p->first)
	     << " transaction " << p->second << dendl;
    MDSTableClient *client = mds->get_table_client(p->first);
    if (client)
      client->got_journaled_agree(p->second, logseg);
  }

  // opened ino?
  if (opened_ino) {
    CInode *in = mds->mdcache->get_inode(opened_ino);
    assert(in);
    dout(10) << "EMetaBlob.replay noting opened inode " << *in << dendl;
    logseg->open_files.push_back(&in->item_open_file);
  }

  // allocated_inos
  if (inotablev) {
    if (mds->inotable->get_version() >= inotablev) {
      dout(10) << "EMetaBlob.replay inotable tablev " << inotablev
	       << " <= table " << mds->inotable->get_version() << dendl;
    } else {
      dout(10) << "EMetaBlob.replay inotable v " << inotablev
	       << " - 1 == table " << mds->inotable->get_version()
	       << " allocated+used " << allocated_ino
	       << " prealloc " << preallocated_inos
	       << dendl;
      if (allocated_ino)
	mds->inotable->replay_alloc_id(allocated_ino);
      if (preallocated_inos.size())
	mds->inotable->replay_alloc_ids(preallocated_inos);

      // [repair bad inotable updates]
      if (inotablev > mds->inotable->get_version()) {
	mds->clog->error() << "journal replay inotablev mismatch "
	    << mds->inotable->get_version() << " -> " << inotablev << "\n";
	mds->inotable->force_replay_version(inotablev);
      }

      assert(inotablev == mds->inotable->get_version());
    }
  }
  if (sessionmapv) {
    if (mds->sessionmap.version >= sessionmapv) {
      dout(10) << "EMetaBlob.replay sessionmap v " << sessionmapv
	       << " <= table " << mds->sessionmap.version << dendl;
    } else if (mds->sessionmap.version + 2 >= sessionmapv) {
      dout(10) << "EMetaBlob.replay sessionmap v " << sessionmapv
	       << " -(1|2) == table " << mds->sessionmap.version
	       << " prealloc " << preallocated_inos
	       << " used " << used_preallocated_ino
	       << dendl;
      Session *session = mds->sessionmap.get_session(client_name);
      if (session) {
	dout(20) << " (session prealloc " << session->info.prealloc_inos << ")" << dendl;
	if (used_preallocated_ino) {
	  if (session->info.prealloc_inos.empty()) {
	    // HRM: badness in the journal
	    mds->clog->warn() << " replayed op " << client_reqs << " on session for "
			     << client_name << " with empty prealloc_inos\n";
	  } else {
	    inodeno_t next = session->next_ino();
	    inodeno_t i = session->take_ino(used_preallocated_ino);
	    if (next != i)
	      mds->clog->warn() << " replayed op " << client_reqs << " used ino " << i
			       << " but session next is " << next << "\n";
	    assert(i == used_preallocated_ino);
	    session->info.used_inos.clear();
	  }
	  mds->sessionmap.projected = ++mds->sessionmap.version;
	}
	if (!preallocated_inos.empty()) {
	  session->info.prealloc_inos.insert(preallocated_inos);
	  mds->sessionmap.projected = ++mds->sessionmap.version;
	}
      } else {
	dout(10) << "EMetaBlob.replay no session for " << client_name << dendl;
	if (used_preallocated_ino)
	  mds->sessionmap.projected = ++mds->sessionmap.version;
	if (!preallocated_inos.empty())
	  mds->sessionmap.projected = ++mds->sessionmap.version;
      }
      assert(sessionmapv == mds->sessionmap.version);
    } else {
      mds->clog->error() << "journal replay sessionmap v " << sessionmapv
			<< " -(1|2) > table " << mds->sessionmap.version << "\n";
      assert(g_conf->mds_wipe_sessions);
      mds->sessionmap.wipe();
      mds->sessionmap.version = mds->sessionmap.projected = sessionmapv;
    }
  }

  // truncating inodes
  for (list<inodeno_t>::iterator p = truncate_start.begin();
       p != truncate_start.end();
       ++p) {
    CInode *in = mds->mdcache->get_inode(*p);
    assert(in);
    mds->mdcache->add_recovered_truncate(in, logseg);
  }
  for (map<inodeno_t,uint64_t>::iterator p = truncate_finish.begin();
       p != truncate_finish.end();
       ++p) {
    LogSegment *ls = mds->mdlog->get_segment(p->second);
    if (ls) {
      CInode *in = mds->mdcache->get_inode(p->first);
      assert(in);
      mds->mdcache->remove_recovered_truncate(in, ls);
    }
  }

  // destroyed inodes
  for (vector<inodeno_t>::iterator p = destroyed_inodes.begin();
       p != destroyed_inodes.end();
       ++p) {
    CInode *in = mds->mdcache->get_inode(*p);
    if (in) {
      dout(10) << "EMetaBlob.replay destroyed " << *p << ", dropping " << *in << dendl;
      mds->mdcache->remove_inode(in);
    } else {
      dout(10) << "EMetaBlob.replay destroyed " << *p << ", not in cache" << dendl;
    }
  }

  // client requests
  for (list<pair<metareqid_t, uint64_t> >::iterator p = client_reqs.begin();
       p != client_reqs.end();
       ++p) {
    if (p->first.name.is_client()) {
      dout(10) << "EMetaBlob.replay request " << p->first << " trim_to " << p->second << dendl;

      // if we allocated an inode, there should be exactly one client request id.
      assert(allocated_ino == inodeno_t() || client_reqs.size() == 1);

      Session *session = mds->sessionmap.get_session(p->first.name);
      if (session) {
	session->add_completed_request(p->first.tid, allocated_ino);
	if (p->second)
	  session->trim_completed_requests(p->second);
      }
    }
  }

  // update segment
  update_segment(logseg);

  assert(g_conf->mds_kill_journal_replay_at != 4);
}

// -----------------------
// ESession

void ESession::update_segment()
{
  _segment->sessionmapv = cmapv;
  if (inos.size() && inotablev)
    _segment->inotablev = inotablev;
}

void ESession::replay(MDS *mds)
{
  if (mds->sessionmap.version >= cmapv) {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap.version 
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap.version
	     << " < " << cmapv << " " << (open ? "open":"close") << " " << client_inst << dendl;
    mds->sessionmap.projected = ++mds->sessionmap.version;
    assert(mds->sessionmap.version == cmapv);
    Session *session;
    if (open) {
      session = mds->sessionmap.get_or_add_session(client_inst);
      mds->sessionmap.set_state(session, Session::STATE_OPEN);
      session->set_client_metadata(client_metadata);
      dout(10) << " opened session " << session->info.inst << dendl;
    } else {
      session = mds->sessionmap.get_session(client_inst.name);
      if (session) { // there always should be a session, but there's a bug
	if (session->connection == NULL) {
	  dout(10) << " removed session " << session->info.inst << dendl;
	  mds->sessionmap.remove_session(session);
	} else {
	  session->clear();    // the client has reconnected; keep the Session, but reset
	  dout(10) << " reset session " << session->info.inst << " (they reconnected)" << dendl;
	}
      } else {
	mds->clog->error() << "replayed stray Session close event for " << client_inst
			  << " from time " << stamp << ", ignoring";
      }
    }
  }
  
  if (inos.size() && inotablev) {
    if (mds->inotable->get_version() >= inotablev) {
      dout(10) << "ESession.replay inotable " << mds->inotable->get_version()
	       << " >= " << inotablev << ", noop" << dendl;
    } else {
      dout(10) << "ESession.replay inotable " << mds->inotable->get_version()
	       << " < " << inotablev << " " << (open ? "add":"remove") << dendl;
      assert(!open);  // for now
      mds->inotable->replay_release_ids(inos);
      assert(mds->inotable->get_version() == inotablev);
    }
  }

  update_segment();
}

// -----------------------
// ESessions

void ESessions::update_segment()
{
  _segment->sessionmapv = cmapv;
}

void ESessions::replay(MDS *mds)
{
  if (mds->sessionmap.version >= cmapv) {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.version
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.version
	     << " < " << cmapv << dendl;
    mds->sessionmap.open_sessions(client_map);
    assert(mds->sessionmap.version == cmapv);
    mds->sessionmap.projected = mds->sessionmap.version;
  }
  update_segment();
}


// -----------------------
// ETableServer

void ETableServer::update_segment()
{
  _segment->tablev[table] = version;
}

void ETableServer::replay(MDS *mds)
{
  MDSTableServer *server = mds->get_table_server(table);
  if (!server)
    return;

  if (server->get_version() >= version) {
    dout(10) << "ETableServer.replay " << get_mdstable_name(table)
	     << " " << get_mdstableserver_opname(op)
	     << " event " << version
	     << " <= table " << server->get_version() << dendl;
    return;
  }
  
  dout(10) << " ETableServer.replay " << get_mdstable_name(table)
	   << " " << get_mdstableserver_opname(op)
	   << " event " << version << " - 1 == table " << server->get_version() << dendl;
  assert(version-1 == server->get_version());

  switch (op) {
  case TABLESERVER_OP_PREPARE:
    server->_prepare(mutation, reqid, bymds);
    server->_note_prepare(bymds, reqid);
    break;
  case TABLESERVER_OP_COMMIT:
    server->_commit(tid);
    server->_note_commit(tid);
    break;
  case TABLESERVER_OP_ROLLBACK:
    server->_rollback(tid);
    server->_note_rollback(tid);
    break;
  case TABLESERVER_OP_SERVER_UPDATE:
    server->_server_update(mutation);
    break;
  default:
    assert(0);
  }
  
  assert(version == server->get_version());
  update_segment();
}


// ---------------------
// ETableClient

void ETableClient::replay(MDS *mds)
{
  dout(10) << " ETableClient.replay " << get_mdstable_name(table)
	   << " op " << get_mdstableserver_opname(op)
	   << " tid " << tid << dendl;
    
  MDSTableClient *client = mds->get_table_client(table);
  if (!client)
    return;

  assert(op == TABLESERVER_OP_ACK);
  client->got_journaled_ack(tid);
}


// -----------------------
// ESnap
/*
void ESnap::update_segment()
{
  _segment->tablev[TABLE_SNAP] = version;
}

void ESnap::replay(MDS *mds)
{
  if (mds->snaptable->get_version() >= version) {
    dout(10) << "ESnap.replay event " << version
	     << " <= table " << mds->snaptable->get_version() << dendl;
    return;
  } 
  
  dout(10) << " ESnap.replay event " << version
	   << " - 1 == table " << mds->snaptable->get_version() << dendl;
  assert(version-1 == mds->snaptable->get_version());

  if (create) {
    version_t v;
    snapid_t s = mds->snaptable->create(snap.dirino, snap.name, snap.stamp, &v);
    assert(s == snap.snapid);
  } else {
    mds->snaptable->remove(snap.snapid);
  }

  assert(version == mds->snaptable->get_version());
}
*/



// -----------------------
// EUpdate

void EUpdate::update_segment()
{
  metablob.update_segment(_segment);

  if (had_slaves)
    _segment->uncommitted_masters.insert(reqid);
}

void EUpdate::replay(MDS *mds)
{
  metablob.replay(mds, _segment);
  
  if (had_slaves) {
    dout(10) << "EUpdate.replay " << reqid << " had slaves, expecting a matching ECommitted" << dendl;
    _segment->uncommitted_masters.insert(reqid);
    set<mds_rank_t> slaves;
    mds->mdcache->add_uncommitted_master(reqid, _segment, slaves, true);
  }
  
  if (client_map.length()) {
    if (mds->sessionmap.version >= cmapv) {
      dout(10) << "EUpdate.replay sessionmap v " << cmapv
	       << " <= table " << mds->sessionmap.version << dendl;
    } else {
      dout(10) << "EUpdate.replay sessionmap " << mds->sessionmap.version
	       << " < " << cmapv << dendl;
      // open client sessions?
      map<client_t,entity_inst_t> cm;
      map<client_t, uint64_t> seqm;
      bufferlist::iterator blp = client_map.begin();
      ::decode(cm, blp);
      mds->server->prepare_force_open_sessions(cm, seqm);
      mds->server->finish_force_open_sessions(cm, seqm);

      assert(mds->sessionmap.version == cmapv);
      mds->sessionmap.projected = mds->sessionmap.version;
    }
  }
}


// ------------------------
// EOpen

void EOpen::update_segment()
{
  // ??
}

void EOpen::replay(MDS *mds)
{
  dout(10) << "EOpen.replay " << dendl;
  metablob.replay(mds, _segment);

  // note which segments inodes belong to, so we don't have to start rejournaling them
  for (vector<inodeno_t>::iterator p = inos.begin();
       p != inos.end();
       ++p) {
    CInode *in = mds->mdcache->get_inode(*p);
    if (!in) {
      dout(0) << "EOpen.replay ino " << *p << " not in metablob" << dendl;
      assert(in);
    }
    _segment->open_files.push_back(&in->item_open_file);
  }
}


// -----------------------
// ECommitted

void ECommitted::replay(MDS *mds)
{
  if (mds->mdcache->uncommitted_masters.count(reqid)) {
    dout(10) << "ECommitted.replay " << reqid << dendl;
    mds->mdcache->uncommitted_masters[reqid].ls->uncommitted_masters.erase(reqid);
    mds->mdcache->uncommitted_masters.erase(reqid);
  } else {
    dout(10) << "ECommitted.replay " << reqid << " -- didn't see original op" << dendl;
  }
}

// -----------------------
// ESlaveUpdate

void ESlaveUpdate::replay(MDS *mds)
{
  MDSlaveUpdate *su;
  switch (op) {
  case ESlaveUpdate::OP_PREPARE:
    dout(10) << "ESlaveUpdate.replay prepare " << reqid << " for mds." << master 
	     << ": applying commit, saving rollback info" << dendl;
    su = new MDSlaveUpdate(origop, rollback, _segment->slave_updates);
    commit.replay(mds, _segment, su);
    mds->mdcache->add_uncommitted_slave_update(reqid, master, su);
    break;

  case ESlaveUpdate::OP_COMMIT:
    su = mds->mdcache->get_uncommitted_slave_update(reqid, master);
    if (su) {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds." << master << dendl;
      mds->mdcache->finish_uncommitted_slave_update(reqid, master);
    } else {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds." << master 
	       << ": ignoring, no previously saved prepare" << dendl;
    }
    break;

  case ESlaveUpdate::OP_ROLLBACK:
    dout(10) << "ESlaveUpdate.replay abort " << reqid << " for mds." << master
	     << ": applying rollback commit blob" << dendl;
    commit.replay(mds, _segment);
    su = mds->mdcache->get_uncommitted_slave_update(reqid, master);
    if (su)
      mds->mdcache->finish_uncommitted_slave_update(reqid, master);
    break;

  default:
    assert(0);
  }
}


// -----------------------
// ESubtreeMap

void ESubtreeMap::replay(MDS *mds) 
{
  if (expire_pos && expire_pos > mds->mdlog->journaler->get_expire_pos())
    mds->mdlog->journaler->set_expire_pos(expire_pos);

  // suck up the subtree map?
  if (mds->mdcache->is_subtrees()) {
    dout(10) << "ESubtreeMap.replay -- i already have import map; verifying" << dendl;
    int errors = 0;

    for (map<dirfrag_t, vector<dirfrag_t> >::iterator p = subtrees.begin();
	 p != subtrees.end();
	 ++p) {
      CDir *dir = mds->mdcache->get_dirfrag(p->first);
      if (!dir) {
	mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree root " << p->first << " not in cache";
	++errors;
	continue;
      }
      
      if (!mds->mdcache->is_subtree(dir)) {
	mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree root " << p->first << " not a subtree in cache";
	++errors;
	continue;
      }
      if (dir->get_dir_auth().first != mds->whoami) {
	mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree root " << p->first
			  << " is not mine in cache (it's " << dir->get_dir_auth() << ")";
	++errors;
	continue;
      }

      for (vector<dirfrag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q)
	mds->mdcache->get_force_dirfrag(*q, true);

      set<CDir*> bounds;
      mds->mdcache->get_subtree_bounds(dir, bounds);
      for (vector<dirfrag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q) {
	CDir *b = mds->mdcache->get_dirfrag(*q);
	if (!b) {
	  mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " bound " << *q << " not in cache";
	++errors;
	  continue;
	}
	if (bounds.count(b) == 0) {
	  mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " bound " << *q << " not a bound in cache";
	++errors;
	  continue;
	}
	bounds.erase(b);
      }
      for (set<CDir*>::iterator q = bounds.begin(); q != bounds.end(); ++q) {
	mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " subtree " << p->first << " has extra bound in cache " << (*q)->dirfrag();
	++errors;
      }
      
      if (ambiguous_subtrees.count(p->first)) {
	if (!mds->mdcache->have_ambiguous_import(p->first)) {
	  mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " is ambiguous but is not in our cache";
	  ++errors;
	}
      } else {
	if (mds->mdcache->have_ambiguous_import(p->first)) {
	  mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			    << " subtree " << p->first << " is not ambiguous but is in our cache";
	  ++errors;
	}
      }
    }
    
    list<CDir*> subs;
    mds->mdcache->list_subtrees(subs);
    for (list<CDir*>::iterator p = subs.begin(); p != subs.end(); ++p) {
      CDir *dir = *p;
      if (dir->get_dir_auth().first != mds->whoami)
	continue;
      if (subtrees.count(dir->dirfrag()) == 0) {
	mds->clog->error() << " replayed ESubtreeMap at " << get_start_off()
			  << " does not include cache subtree " << dir->dirfrag();
	++errors;
      }
    }

    if (errors) {
      dout(0) << "journal subtrees: " << subtrees << dendl;
      dout(0) << "journal ambig_subtrees: " << ambiguous_subtrees << dendl;
      mds->mdcache->show_subtrees();
      assert(!g_conf->mds_debug_subtrees || errors == 0);
    }
    return;
  }

  dout(10) << "ESubtreeMap.replay -- reconstructing (auth) subtree spanning tree" << dendl;
  
  // first, stick the spanning tree in my cache
  //metablob.print(*_dout);
  metablob.replay(mds, _segment);
  
  // restore import/export maps
  for (map<dirfrag_t, vector<dirfrag_t> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = mds->mdcache->get_dirfrag(p->first);
    assert(dir);
    if (ambiguous_subtrees.count(p->first)) {
      // ambiguous!
      mds->mdcache->add_ambiguous_import(p->first, p->second);
      mds->mdcache->adjust_bounded_subtree_auth(dir, p->second,
						mds_authority_t(mds->get_nodeid(), mds->get_nodeid()));
    } else {
      // not ambiguous
      mds->mdcache->adjust_bounded_subtree_auth(dir, p->second, mds->get_nodeid());
    }
  }

  mds->mdcache->recalc_auth_bits(true);

  mds->mdcache->show_subtrees();
}



// -----------------------
// EFragment

void EFragment::replay(MDS *mds)
{
  dout(10) << "EFragment.replay " << op_name(op) << " " << ino << " " << basefrag << " by " << bits << dendl;

  list<CDir*> resultfrags;
  list<MDSInternalContextBase*> waiters;
  list<frag_t> old_frags;

  // in may be NULL if it wasn't in our cache yet.  if it's a prepare
  // it will be once we replay the metablob , but first we need to
  // refragment anything we already have in the cache.
  CInode *in = mds->mdcache->get_inode(ino);

  switch (op) {
  case OP_PREPARE:
    mds->mdcache->add_uncommitted_fragment(dirfrag_t(ino, basefrag), bits, orig_frags, _segment, &rollback);
    // fall-thru
  case OP_ONESHOT:
    if (in)
      mds->mdcache->adjust_dir_fragments(in, basefrag, bits, resultfrags, waiters, true);
    break;

  case OP_ROLLBACK:
    if (in) {
      in->dirfragtree.get_leaves_under(basefrag, old_frags);
      if (orig_frags.empty()) {
	// old format EFragment
	mds->mdcache->adjust_dir_fragments(in, basefrag, -bits, resultfrags, waiters, true);
      } else {
	for (list<frag_t>::iterator p = orig_frags.begin(); p != orig_frags.end(); ++p)
	  mds->mdcache->force_dir_fragment(in, *p);
      }
    }
    mds->mdcache->rollback_uncommitted_fragment(dirfrag_t(ino, basefrag), old_frags);
    break;

  case OP_COMMIT:
  case OP_FINISH:
    mds->mdcache->finish_uncommitted_fragment(dirfrag_t(ino, basefrag), op);
    break;

  default:
    assert(0);
  }

  metablob.replay(mds, _segment);
  if (in && g_conf->mds_debug_frag)
    in->verify_dirfrags();
}




// =========================================================================

// -----------------------
// EExport

void EExport::replay(MDS *mds)
{
  dout(10) << "EExport.replay " << base << dendl;
  metablob.replay(mds, _segment);
  
  CDir *dir = mds->mdcache->get_dirfrag(base);
  assert(dir);
  
  set<CDir*> realbounds;
  for (set<dirfrag_t>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = mds->mdcache->get_dirfrag(*p);
    assert(bd);
    realbounds.insert(bd);
  }

  // adjust auth away
  mds->mdcache->adjust_bounded_subtree_auth(dir, realbounds, CDIR_AUTH_UNDEF);

  mds->mdcache->try_trim_non_auth_subtree(dir);
}


// -----------------------
// EImportStart

void EImportStart::update_segment()
{
  _segment->sessionmapv = cmapv;
}

void EImportStart::replay(MDS *mds)
{
  dout(10) << "EImportStart.replay " << base << " bounds " << bounds << dendl;
  //metablob.print(*_dout);
  metablob.replay(mds, _segment);

  // put in ambiguous import list
  mds->mdcache->add_ambiguous_import(base, bounds);

  // set auth partially to us so we don't trim it
  CDir *dir = mds->mdcache->get_dirfrag(base);
  assert(dir);

  set<CDir*> realbounds;
  for (vector<dirfrag_t>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = mds->mdcache->get_dirfrag(*p);
    assert(bd);
    if (!bd->is_subtree_root())
      bd->state_clear(CDir::STATE_AUTH);
    realbounds.insert(bd);
  }

  mds->mdcache->adjust_bounded_subtree_auth(dir, realbounds,
					    mds_authority_t(mds->get_nodeid(), mds->get_nodeid()));

  // open client sessions?
  if (mds->sessionmap.version >= cmapv) {
    dout(10) << "EImportStart.replay sessionmap " << mds->sessionmap.version 
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "EImportStart.replay sessionmap " << mds->sessionmap.version 
	     << " < " << cmapv << dendl;
    map<client_t,entity_inst_t> cm;
    bufferlist::iterator blp = client_map.begin();
    ::decode(cm, blp);
    mds->sessionmap.open_sessions(cm);
    assert(mds->sessionmap.version == cmapv);
    mds->sessionmap.projected = mds->sessionmap.version;
  }
  update_segment();
}

// -----------------------
// EImportFinish

void EImportFinish::replay(MDS *mds)
{
  if (mds->mdcache->have_ambiguous_import(base)) {
    dout(10) << "EImportFinish.replay " << base << " success=" << success << dendl;
    if (success) {
      mds->mdcache->finish_ambiguous_import(base);
    } else {
      CDir *dir = mds->mdcache->get_dirfrag(base);
      assert(dir);
      vector<dirfrag_t> bounds;
      mds->mdcache->get_ambiguous_import_bounds(base, bounds);
      mds->mdcache->adjust_bounded_subtree_auth(dir, bounds, CDIR_AUTH_UNDEF);
      mds->mdcache->cancel_ambiguous_import(dir);
      mds->mdcache->try_trim_non_auth_subtree(dir);
   }
  } else {
    dout(10) << "EImportFinish.replay " << base << " success=" << success
	     << " on subtree not marked as ambiguous" 
	     << dendl;
    assert(0 == "this shouldn't happen unless this is an old journal");
  }
}


// ------------------------
// EResetJournal

void EResetJournal::replay(MDS *mds)
{
  dout(1) << "EResetJournal" << dendl;

  mds->sessionmap.wipe();
  mds->inotable->replay_reset();

  if (mds->mdsmap->get_root() == mds->whoami) {
    CDir *rootdir = mds->mdcache->get_root()->get_or_open_dirfrag(mds->mdcache, frag_t());
    mds->mdcache->adjust_subtree_auth(rootdir, mds->whoami);   
  }

  CDir *mydir = mds->mdcache->get_myin()->get_or_open_dirfrag(mds->mdcache, frag_t());
  mds->mdcache->adjust_subtree_auth(mydir, mds->whoami);   

  mds->mdcache->recalc_auth_bits(true);

  mds->mdcache->show_subtrees();
}



void ENoOp::replay(MDS *mds)
{
  dout(4) << "ENoOp::replay, " << pad_size << " bytes skipped in journal" << dendl;
}

/**
 * If re-formatting an old journal that used absolute log position
 * references as segment sequence numbers, use this function to update
 * it.
 *
 * @param mds
 * MDS instance, just used for logging
 * @param old_to_new
 * Map of old journal segment segment sequence numbers to new journal segment sequence numbers
 *
 * @return
 * True if the event was modified.
 */
bool EMetaBlob::rewrite_truncate_finish(MDS const *mds,
    std::map<log_segment_seq_t, log_segment_seq_t> const &old_to_new)
{
  bool modified = false;
  map<inodeno_t, log_segment_seq_t> new_trunc_finish;
  for (std::map<inodeno_t, log_segment_seq_t>::iterator i = truncate_finish.begin();
      i != truncate_finish.end(); ++i) {
    if (old_to_new.count(i->second)) {
      dout(20) << __func__ << " applying segment seq mapping "
        << i->second << " -> " << old_to_new.find(i->second)->second << dendl;
      new_trunc_finish[i->first] = old_to_new.find(i->second)->second;
      modified = true;
    } else {
      dout(20) << __func__ << " no segment seq mapping found for "
        << i->second << dendl;
      new_trunc_finish[i->first] = i->second;
    }
  }
  truncate_finish = new_trunc_finish;

  return modified;
}
