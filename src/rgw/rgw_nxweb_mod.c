
#include <errno.h>

#include "nxweb/nxweb.h"
#include "rgw_nxweb.h"


nxweb_result rgw_nxweb_cb(nxweb_http_server_connection* conn, nxweb_http_request* req, nxweb_http_response* resp);

NXWEB_DEFINE_HANDLER(rgw, .on_request=rgw_nxweb_cb,
        .flags=NXWEB_HANDLE_ANY|NXWEB_INWORKER|NXWEB_PARSE_PARAMETERS|NXWEB_PARSE_COOKIES);


int rgw_nxweb_init(int port)
{
  char buf[32];
  snprintf(buf, sizeof(buf), ":%d", port);
  if (nxweb_listen(buf, 4096))
	  return -EIO;

  NXWEB_HANDLER_SETUP(my_rgw, "/", rgw, .priority=1000, .vhost = "", .filters={
  #ifdef WITH_ZLIB
    nxweb_gzip_filter_setup(4, 0), // null cache_dir as we don't need caching
  #endif
  });

  nxweb_error_log_level=NXWEB_LOG_INFO;

  return 0;
}

void rgw_nxweb_run()
{
  nxweb_run();
}

