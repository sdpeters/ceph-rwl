/* Include the required headers from httpd */
#include "httpd.h"
#include "http_config.h"
#include "http_core.h"
#include "http_protocol.h"
#include "http_request.h"

/* Define prototypes of our functions in this module */
static void register_hooks(apr_pool_t *pool);
static void rgw_mod_init(apr_pool_t *p, server_rec *s);
static int rgw_mod_handler(request_rec *r);

/* Define our module as an entity and assign a function for registering hooks  */

module AP_MODULE_DECLARE_DATA   rgw_module =
{
    STANDARD20_MODULE_STUFF,
    NULL,            // Per-directory configuration handler
    NULL,            // Merge handler for per-directory configurations
    NULL,            // Per-server configuration handler
    NULL,            // Merge handler for per-server configurations
    NULL,            // Any directives we may have for httpd
    register_hooks   // Our hook registering function
};


/* register_hooks: Adds a hook to the httpd process */
static void register_hooks(apr_pool_t *pool) 
{
    /* Hook the initialization handler */
    ap_hook_child_init(rgw_mod_init, NULL, NULL, APR_HOOK_LAST);

    /* Hook the request handler */
    ap_hook_handler(rgw_mod_handler, NULL, NULL, APR_HOOK_LAST);
}

extern int rgw_process_init(int argc, const char **argv);

static void rgw_mod_init(apr_pool_t *p, server_rec *s)
{
//  rgw_process_init(0, NULL);
}

static int rgw_mod_handler(request_rec *r)
{
    /* First off, we need to check if this is a call for the "rgw" handler.
     * If it is, we accept it and do our things, it not, we simply return DECLINED,
     * and Apache will try somewhere else.
     */
    if (!r->handler || strcmp(r->handler, "rgw-handler")) return (DECLINED);
    
    // The first thing we will do is write a simple "Hello, world!" back to the client.
    ap_rputs("Hello, rgw world!<br/>", r);
    return OK;
}
