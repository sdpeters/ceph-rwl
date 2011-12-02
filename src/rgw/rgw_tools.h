#ifndef CEPH_RGW_TOOLS_H
#define CEPH_RGW_TOOLS_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"

class RGWAccess;


int rgw_put_obj(RGWAccess *access, string& uid, rgw_bucket& bucket, string& oid, const char *data, size_t size);
int rgw_get_obj(RGWAccess *access, void *ctx, rgw_bucket& bucket, string& key, bufferlist& bl);

int rgw_tools_init(CephContext *cct);
const char *rgw_find_mime_by_ext(string& ext);

#endif
