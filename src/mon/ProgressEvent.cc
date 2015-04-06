
#include "common/debug.h"

#include "ProgressEvent.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix *_dout << "mon.ProgressEvent " << __func__ << " "

ProgressEvent::ProgressEvent(
            const std::string &description_,
            uint32_t target_,
            const EventCode event_code_)
 : event_code(event_code_), description(description_),
   finished(false), target(target_)
{
}

void ProgressEvent::tick(const OSDMap &osd_map, const PGMap &pg_map)
{
        switch(event_code) {
            case OSD_OUT:
                tick_osd_out(osd_map, pg_map);
                break;
            default:
                assert(0);
        }
}

void ProgressEvent::tick_osd_out(const OSDMap &osd_map, const PGMap &pg_map)
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

    if (pgs_working.empty()) {
        finished = true;
    }
}

bool ProgressEvent::is_complete()
{
    return finished;
}

void ProgressEvent::init(const OSDMap &osd_map, const PGMap &pg_map)
{
    if (event_code == OSD_OUT) {
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
    } else {
        assert(0);
    }
}

float ProgressEvent::progress(const OSDMap &osd_map, const PGMap &pg_map)
{
    if (finished) {
        return 1.0;
    }

    return (float)(pgs_done.size()) / float(pgs_working.size() + pgs_done.size());
}

