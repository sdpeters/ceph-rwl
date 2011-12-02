#include "libradosgw.hpp"

#include "rgw_rados.h"
#include "rgw_cache.h"


namespace libradosgw {

  class StoreImpl {
    RGWRados *rados;
  public:

    StoreImpl() : rados(NULL) {}

    int init(CephContext *cct) {
      int use_cache = cct->_conf->rgw_cache_enabled;

      if (use_cache) {
        rados = new RGWRados;
      } else {
	rados = new RGWCache<RGWRados>;
      }

      int ret = rados->initialize(cct);

      return ret;
    }

    void shutdown() {
      if (!rados)
	return;

      rados->finalize();
      rados = NULL;
    }

    int get_account(string& name, Account& account) {}
    int user_by_name(string& name, User& user) {}
    int user_by_email(string& email, User& user) {}
    int user_by_access_key(string& access_key, User& user) {}
  };


  int Store::init(CephContext *cct) {
    impl = new StoreImpl;
    return impl->init(cct);
  }

  void Store::shutdown() {
    impl->shutdown();
    delete impl;
    impl = NULL;
  }

  int Store::get_account(string& name, Account& account) {
    return impl->get_account(name, account);
  }

  int Store::user_by_name(string& name, User& user) {
    return impl->user_by_name(name, user);
  }

  int Store::user_by_email(string& email, User& user) {
    return impl->user_by_email(email, user);
  }

  int Store::user_by_access_key(string& access_key, User& user) {
    return impl->user_by_access_key(access_key, user);
  }
}
