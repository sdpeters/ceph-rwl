#ifndef CEPH_RGW_NXWEB_H
#define CEPH_RGW_NXWEB_H


#ifdef __cplusplus
extern "C" {
#endif

int rgw_nxweb_init(int port);
void rgw_nxweb_run(void);

#ifdef __cplusplus
}
#endif
#endif
