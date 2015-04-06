// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"

#include "ProgressEvent.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix *_dout << "mon.ProgressEvent " << __func__ << " "

ProgressEvent::ProgressEvent(
    const std::string &description_,
    const EventCode event_code_)
: event_code(event_code_), description(description_),
  finished(false)
{
}

bool ProgressEvent::is_complete()
{
  return finished;
}

float PgProgressEvent::progress(const OSDMap &osd_map, const PGMap &pg_map)
{
  if (finished) {
    return 1.0;
  }

  if (pgs_working.size() + pgs_done.size() == 0) {
    // We are a no-op, avoid div by zero
    return 1.0;
  }

  return (float)(pgs_done.size()) / float(pgs_working.size() + pgs_done.size());
}

void ProgressOsdOut::tick(const OSDMap &osd_map, const PGMap &pg_map)
{
  if (is_complete()) {
    return;
  }

  // If the OSD is now in, then we're done, go home
#if 0
  FIXME to work properly we need to note epochs and not apply
    this condition until we're seeing a later map than we
    started iwth
    if (osd_map.get_weight(target) != OSD_OUT) {
      // FIXME handle case of OSD going away (i.e. DNE in map)
      finished = true;
      dout(4) << "it came back in" << dendl;
    }
#endif

  std::list<pg_t> pgs_newly_done;
  std::vector<pg_t> pgs_still_working;
  // If any working PGs are now healthy in their new position,
  // move them to done
  for (std::vector<pg_t>::iterator i = pgs_working.begin();
      i != pgs_working.end(); ++i) {
    const pg_stat_t &stat = pg_map.pg_stat.find(*i)->second;
    bool in_up = std::find(stat.up.begin(), stat.up.end(), target)!=stat.up.end();
    bool in_acting = std::find(stat.acting.begin(), stat.acting.end(), target)!=stat.acting.end();
    bool is_done = false;
    if (!in_up && !in_acting) {
      if (stat.state & PG_STATE_CLEAN) {
        // FIXME: should actually be checking last_epoch_clean vs.
        // the epoch when we started this ProgressEvent -- need
        // to modify ProgressEvent structure to store this
        // initial state per PG
        is_done = true;
      }
    }

    if (is_done) {
      pgs_newly_done.push_back(*i);
    } else {
      pgs_still_working.push_back(*i);
    }
  }

  dout(4) << "pgs_newly_done: " << pgs_newly_done.size() << dendl;

  pgs_working = pgs_still_working;
  for (std::list<pg_t>::iterator i = pgs_newly_done.begin();
      i != pgs_newly_done.end(); ++i) {
    pgs_done.push_back(*i);
  }

  if (!finished && pgs_working.empty()) {
    dout(4) << "we finished!" << dendl;;
    finished = true;
  }
}

void ProgressOsdOut::init(
    const OSDMap &osd_map,
    const OSDMap &pending_osd_map,
    const PGMap &pg_map)
{
  // Find PGs that map to this OSD -- intrinsically not a fast
  // operations because everything is designed to do PG->OSD mappings,
  // not vice versa
  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator i = pg_map.pg_stat.begin();
      i != pg_map.pg_stat.end(); ++i) {
    int up_primary, acting_primary;
    vector<int> up, acting;
    osd_map.pg_to_up_acting_osds(i->first, &up, &up_primary,
        &acting, &acting_primary);
    bool in_up = std::find(up.begin(), up.end(), target)!=up.end();
    bool in_acting = std::find(acting.begin(), acting.end(), target)!=acting.end();
    if (in_up || in_acting) {
      pgs_working.push_back(i->first);
    }
  }

  dout(4) << "found " << pgs_working.size() << " affected PGs" << dendl;
}

void ProgressOsdIn::init(
    const OSDMap &osd_map,
    const OSDMap &pending_osd_map,
    const PGMap &pg_map)
{
  // We need to see the *new* OSD map, learn which PGs are allocated
  // to the newly in OSD.  We can't look at the PGMap because it won't
  // have been updated yet.
  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator i = pg_map.pg_stat.begin();
      i != pg_map.pg_stat.end(); ++i) {
    int up_primary, acting_primary;
    vector<int> up, acting;
    pending_osd_map.pg_to_up_acting_osds(i->first, &up, &up_primary,
        &acting, &acting_primary);
    bool in_up = std::find(up.begin(), up.end(), target)!=up.end();
    bool in_acting = std::find(acting.begin(), acting.end(), target)!=acting.end();
    if (in_up || in_acting) {
      pgs_working.push_back(i->first);
    }
  }
}

void ProgressOsdIn::tick(const OSDMap &osd_map, const PGMap &pg_map)
{
  if (is_complete()) {
    return;
  }

  // If the OSD is now in, then we're done, go home
#if 0
  FIXME to work properly we need to note epochs and not apply
    this condition until we're seeing a later map than we
    started iwth
    if (osd_map.get_weight(target) != OSD_OUT) {
      // FIXME handle case of OSD going away (i.e. DNE in map)
      finished = true;
      dout(4) << "it came back in" << dendl;
    }
#endif

  // FIXME generalise this logic between OsdIn and OsdOut and parameterise
  // with a is_pg_ready function for the inner loop or so.
  std::list<pg_t> pgs_newly_done;
  std::vector<pg_t> pgs_still_working;
  // If any working PGs are now healthy in their new position,
  // move them to done
  for (std::vector<pg_t>::iterator i = pgs_working.begin();
      i != pgs_working.end(); ++i) {
    const pg_stat_t &stat = pg_map.pg_stat.find(*i)->second;
    //bool in_up = std::find(stat.up.begin(), stat.up.end(), target)!=stat.up.end();
    bool in_acting = std::find(stat.acting.begin(), stat.acting.end(), target)!=stat.acting.end();
    bool is_done = false;
    if (in_acting) {
      if (stat.state & PG_STATE_CLEAN) {
        // FIXME: should actually be checking last_epoch_clean vs.
        // the epoch when we started this ProgressEvent -- need
        // to modify ProgressEvent structure to store this
        // initial state per PG
        is_done = true;
      }
    }

    if (is_done) {
      pgs_newly_done.push_back(*i);
    } else {
      pgs_still_working.push_back(*i);
    }
  }

  dout(4) << "pgs_newly_done: " << pgs_newly_done.size() << dendl;

  pgs_working = pgs_still_working;
  for (std::list<pg_t>::iterator i = pgs_newly_done.begin();
      i != pgs_newly_done.end(); ++i) {
    pgs_done.push_back(*i);
  }

  if (pgs_working.empty()) {
    finished = true;
  }
}

void ProgressScrub::init(
      const OSDMap &osd_map,
      const OSDMap &pending_osd_map,
      const PGMap &pg_map)
{
  // Take our baseline timestamp as the latest one in the PGMap, to avoid
  // any weirdness if OSD clocks don't match our local clock
  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator i = pg_map.pg_stat.begin();
      i != pg_map.pg_stat.end(); ++i) {
    if (i->second.last_scrub_stamp > start_time) {
      start_time = i->second.last_scrub_stamp;
    }
  }

  dout(4) << "start_time: " << start_time << dendl;

  // Find PGs whose acting primary is targeted by the scrub
  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator i = pg_map.pg_stat.begin();
    i != pg_map.pg_stat.end(); ++i) {
    // FIXME I'm assuming OSD will scrub anything it is *acting* as the
    // primary for, rather than only thing which it is truly the primary
    // for, but haven't really checked.
    // FIXME: race between which PGs the OSD has at the time it receives
    // the scrub message vs. which PGs we think it has when we do this
    // calculation.  Maybe we need to do this calculation before messaging
    // the OSD, then on subsequent osdmap updates, remove anything that
    // has been moved away from this OSD out of our working list.
    int up_primary, acting_primary;
    vector<int> up, acting;
    osd_map.pg_to_up_acting_osds(i->first, &up, &up_primary,
        &acting, &acting_primary);

    bool acting_in_target_set = std::find(
        osd_ids.begin(), osd_ids.end(), acting_primary)!=osd_ids.end();
    if (acting_in_target_set) {
      dout(4) << i->first << ": " << acting_primary << dendl;
      pgs_working.push_back(i->first);
    }
  }

  dout(4) << "osds: " << osd_ids.size() << dendl;
  dout(4) << "pgs_working: " << pgs_working.size() << dendl;

}

void ProgressScrub::tick(const OSDMap &osd_map, const PGMap &pg_map)
{
  if (is_complete()) {
    return;
  }

  std::list<pg_t> pgs_newly_done;
  std::vector<pg_t> pgs_still_working;
  for (std::vector<pg_t>::iterator i = pgs_working.begin();
      i != pgs_working.end(); ++i) {
    const pg_stat_t &stat = pg_map.pg_stat.find(*i)->second;

    utime_t scrub_stamp;
    if (deep) {
      scrub_stamp = stat.last_deep_scrub_stamp;
    } else {
      scrub_stamp = stat.last_scrub_stamp;
    }

    const bool is_done = scrub_stamp > start_time;
    if (is_done) {
      pgs_newly_done.push_back(*i);
    } else {
      pgs_still_working.push_back(*i);
    }
  }

  dout(4) << "pgs_newly_done: " << pgs_newly_done.size() << dendl;

  pgs_working = pgs_still_working;
  for (std::list<pg_t>::iterator i = pgs_newly_done.begin();
      i != pgs_newly_done.end(); ++i) {
    pgs_done.push_back(*i);
  }

  if (pgs_working.empty()) {
    finished = true;
  }
}

