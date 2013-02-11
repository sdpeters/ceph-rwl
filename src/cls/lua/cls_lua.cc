/*
 * Lua Bindings for RADOS Object Class
 */
#include <errno.h>
#include <setjmp.h>
#include <string>
#include <sstream>
#include <lua.hpp>
#include "include/types.h"
#include "objclass/objclass.h"
#include "cls/lua/cls_lua.h"

CLS_VER(1,0)
CLS_NAME(lua)

cls_handle_t h_class;
cls_method_handle_t h_eval;

/*
 * Jump point for recovering from Lua panic.
 */
static jmp_buf cls_lua_panic_jump;

/*
 * Handle Lua panic
 */
static int cls_lua_atpanic(lua_State *lua)
{
  CLS_ERR("error: Lua panic: %s", lua_tostring(lua, -1));
  longjmp(cls_lua_panic_jump, 1);
  return 0;
}

struct clslua_err {
  bool error;
  int ret;
};

struct clslua_hctx {
  struct clslua_err error;
  bufferlist *inbl;
  bufferlist *outbl;
  cls_method_context_t *hctx;
  int ret;
};

/* Lua registry key for method context */
static char clslua_hctx_reg_key;

/*
 * Grabs the full method handler context
 */
static clslua_hctx *__clslua_get_hctx(lua_State *L)
{
  /* lookup registry value */
  lua_pushlightuserdata(L, &clslua_hctx_reg_key);
  lua_gettable(L, LUA_REGISTRYINDEX);

  /* check cls_lua assumptions */
  assert(!lua_isnil(L, -1));
  assert(lua_type(L, -1) == LUA_TLIGHTUSERDATA);

  /* cast and cleanup stack */
  clslua_hctx *hctx = (struct clslua_hctx *)lua_touserdata(L, -1);
  lua_pop(L, 1);

  return hctx;
}

/*
 * Get the method context out of the registry. This is called at the beginning
 * of each clx_cxx_* wrapper, and must be set before there is any chance a Lua
 * script calling a 'cls' module function that requires it.
 */
static cls_method_context_t clslua_get_hctx(lua_State *L)
{
  struct clslua_hctx *hctx = __clslua_get_hctx(L);
  return *hctx->hctx;
}

/*
 * Returns a reference to cls_lua error state from registry.
 */
struct clslua_err *clslua_checkerr(lua_State *L)
{
  struct clslua_hctx *hctx = __clslua_get_hctx(L);
  struct clslua_err *err = &hctx->error;
  return err;
}


/* Registry key for real `pcall` function */
static char clslua_pcall_reg_key;

/*
 * Wrap Lua pcall to check for errors thrown by cls_lua (e.g. I/O errors or
 * bufferlist decoding errors). The global error is cleared before returning
 * to the caller.
 */
static int clslua_pcall(lua_State *L)
{
  int nargs = lua_gettop(L);
  lua_pushlightuserdata(L, &clslua_pcall_reg_key);
  lua_gettable(L, LUA_REGISTRYINDEX);
  lua_insert(L, 1);
  lua_call(L, nargs, LUA_MULTRET);
  struct clslua_err *err = clslua_checkerr(L);
  assert(err);
  if (err->error) {
    err->error = false;
    lua_pushinteger(L, err->ret);
    lua_insert(L, -2);
  }
  return lua_gettop(L);
}


/*
 * cls_log
 */
static int clslua_log(lua_State *L)
{
  int nargs = lua_gettop(L);

  if (!nargs)
    return 0;

  int loglevel = LOG_LEVEL_DEFAULT;
  bool custom_ll = false;

  /* check if first arg can be a log level */
  if (nargs > 1 && lua_isnumber(L, 1)) {
    int ll = (int)lua_tonumber(L, 1);
    if (ll >= 0) {
      loglevel = ll;
      custom_ll = true;
    }
  }

  /* check space for args and seperators (" ") */
  int nelems = ((nargs - (custom_ll ? 1 : 0)) * 2) - 1;
  luaL_checkstack(L, nelems, "rados.log(..)");

  for (int i = custom_ll ? 2 : 1; i <= nargs; i++) {
    const char *part = lua_tostring(L, i);
    if (!part) {
      if (lua_type(L, i) == LUA_TBOOLEAN)
        part = lua_toboolean(L, i) ? "true" : "false";
      else
        part = luaL_typename(L, i);
    }
    lua_pushstring(L, part);
    if ((i+1) <= nargs)
      lua_pushstring(L, " ");
  }

  /* join string parts and send to Ceph/reply log */
  lua_concat(L, nelems);
  CLS_LOG(loglevel, "%s", lua_tostring(L, -1));

  /* concat leaves result at top of stack */
  return 1;
}

static char clslua_registered_handle_reg_key;

/*
 * Register a function to be used as a handler target
 */
static int clslua_register(lua_State *L)
{
  luaL_checktype(L, 1, LUA_TFUNCTION);

  /* get table of registered handlers */
  lua_pushlightuserdata(L, &clslua_registered_handle_reg_key);
  lua_gettable(L, LUA_REGISTRYINDEX);
  assert(lua_type(L, -1) == LUA_TTABLE);

  /* lookup function argument */
  lua_pushvalue(L, 1);
  lua_gettable(L, -2);

  if (lua_isnil(L, -1)) {
    lua_pushvalue(L, 1);
    lua_pushvalue(L, 1);
    lua_settable(L, -4);
  } else {
    lua_pushstring(L, "Cannot register handler more than once");
    return lua_error(L);
  }

  return 0;
}

/*
 * Check if a function is registered as a handler
 */
static void clslua_check_registered_handler(lua_State *L)
{
  luaL_checktype(L, -1, LUA_TFUNCTION);

  /* get table of registered handlers */
  lua_pushlightuserdata(L, &clslua_registered_handle_reg_key);
  lua_gettable(L, LUA_REGISTRYINDEX);
  assert(lua_type(L, -1) == LUA_TTABLE);

  /* lookup function argument */
  lua_pushvalue(L, -2);
  lua_gettable(L, -2);

  if (!lua_rawequal(L, -1, -3)) {
    lua_pushstring(L, "Handler is not registered");
    lua_error(L);
  }

  lua_pop(L, 2);
}

/*
 * Handle result of a cls_cxx_* call. If @ok is non-zero then we return with
 * the number of Lua return arguments on the stack. Otherwise we save error
 * information in the registry and throw a Lua error.
 */
static int clslua_opresult(lua_State *L, int ok, int ret, int nargs)
{
  struct clslua_err *err = clslua_checkerr(L);

  assert(err);
  if (err->error) {
    CLS_ERR("error: cls_lua state machine: unexpected error");
    assert(0);
  }

  /* everything is cherry */
  if (ok)
    return nargs;

  /* set error in registry */
  err->error = true;
  err->ret = ret;

  /* push error message */
  lua_pushfstring(L, "%s", strerror(ret));

  return lua_error(L);
}

/*
 * cls_cxx_create
 */
static int clslua_create(lua_State *lua)
{
  cls_method_context_t hctx = clslua_get_hctx(lua);
  int exclusive = lua_toboolean(lua, 1);

  int ret = cls_cxx_create(hctx, exclusive);
  return clslua_opresult(lua, (ret == 0), ret, 0);
}

/*
 * cls_cxx_remove
 */
static int clslua_remove(lua_State *lua)
{
  cls_method_context_t hctx = clslua_get_hctx(lua);

  int ret = cls_cxx_remove(hctx);
  return clslua_opresult(lua, (ret == 0), ret, 0);
}

/*
 * cls_cxx_stat
 */
static int clslua_stat(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);

  uint64_t size;
  time_t mtime;
  int ret = cls_cxx_stat(hctx, &size, &mtime);
  if (!ret) {
    lua_pushinteger(L, size);
    lua_pushinteger(L, mtime);
  }
  return clslua_opresult(L, (ret == 0), ret, 2);
}

/*
 * cls_cxx_read
 */
static int clslua_read(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  int offset = luaL_checkint(L, 1);
  int length = luaL_checkint(L, 2);
  bufferlist *bl = clslua_pushbufferlist(L, NULL);
  int ret = cls_cxx_read(hctx, offset, length, bl);
  return clslua_opresult(L, (ret >= 0), ret, 1);
}

/*
 * cls_cxx_write
 */
static int clslua_write(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  int offset = luaL_checkint(L, 1);
  int length = luaL_checkint(L, 2);
  bufferlist *bl = clslua_checkbufferlist(L, 3);
  int ret = cls_cxx_write(hctx, offset, length, bl);
  return clslua_opresult(L, (ret == 0), ret, 0);
}

/*
 * cls_cxx_getxattr
 */
static int clslua_getxattr(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *name = luaL_checkstring(L, 1);
  bufferlist  *bl = clslua_pushbufferlist(L, NULL);
  int ret = cls_cxx_getxattr(hctx, name, bl);
  return clslua_opresult(L, (ret >= 0), ret, 1);
}

/*
 * cls_cxx_setxattr
 */
static int clslua_setxattr(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *name = luaL_checkstring(L, 1);
  bufferlist *bl = clslua_checkbufferlist(L, 2);
  int ret = cls_cxx_setxattr(hctx, name, bl);
  return clslua_opresult(L, (ret == 0), ret, 1);
}

/*
 * cls_cxx_map_get_val
 */
static int clslua_map_get_val(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *key = luaL_checkstring(L, 1);
  bufferlist  *bl = clslua_pushbufferlist(L, NULL);
  int ret = cls_cxx_map_get_val(hctx, key, bl);
  return clslua_opresult(L, (ret == 0), ret, 1);
}

/*
 * cls_cxx_map_set_val
 */
static int clslua_map_set_val(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *key = luaL_checkstring(L, 1);
  bufferlist *val = clslua_checkbufferlist(L, 2);
  int ret = cls_cxx_map_set_val(hctx, key, val);
  return clslua_opresult(L, (ret == 0), ret, 0);
}

/*
 * cls_cxx_map_clear
 */
static int clslua_map_clear(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  int ret = cls_cxx_map_clear(hctx);
  return clslua_opresult(L, (ret == 0), ret, 0);
}

/*
 * Functions registered in the 'cls' module.
 */
static const luaL_Reg clslua_lib[] = {
  {"log", clslua_log},
  {"register", clslua_register},
  {"create", clslua_create},
  {"remove", clslua_remove},
  {"stat", clslua_stat},
  {"read", clslua_read},
  {"write", clslua_write},
  {"map_clear", clslua_map_clear},
  {"map_get_val", clslua_map_get_val},
  {"map_set_val", clslua_map_set_val},
  {"getxattr", clslua_getxattr},
  {"setxattr", clslua_setxattr},
  {NULL, NULL}
};

/*
 * Set int const in table at top of stack
 */
#define SET_INT_CONST(var) do { \
  lua_pushinteger(L, var); \
  lua_setfield(L, -2, #var); \
} while (0)

/*
 * Setup the execution environment. Our sandbox currently is not
 * sophisticated. With a new Lua state per-request we don't need to work about
 * users stepping on each other, but we do rip out access to the local file
 * system. All this will change when/if we decide to use some shared Lua
 * states, most likely for performance reasons.
 */
static void clslua_setup_env(lua_State *L)
{
  /*
   * TODO: luaL_openlibs will create a non-sandboxed environment. a fairly
   * decent sandbox is omitted below in the `#if 0` block. Some method to
   * configure the sandbox used should be created (e.g. g_conf->cls_lua_use_sandbox).
   */
  luaL_openlibs(L);

#if 0
  /* load base Lua library */
  lua_pushcfunction(L, luaopen_base);
  lua_pushstring(L, "");
  lua_call(L, 1, 0);

  /* mask unsafe */
  lua_pushnil(L);
  lua_setglobal(L, "loadfile");

  /* mask unsafe */
  lua_pushnil(L);
  lua_setglobal(L, "dofile");

  /* not integrated into our error handling */
  lua_pushnil(L);
  lua_setglobal(L, "xpcall");

  /* load table library */
  lua_pushcfunction(L, luaopen_table);
  lua_pushstring(L, LUA_TABLIBNAME);
  lua_call(L, 1, 0);

  /* load string library */
  lua_pushcfunction(L, luaopen_string);
  lua_pushstring(L, LUA_STRLIBNAME);
  lua_call(L, 1, 0);

  /* load math library */
  lua_pushcfunction(L, luaopen_math);
  lua_pushstring(L, LUA_MATHLIBNAME);
  lua_call(L, 1, 0);

  /* load debug library */
  lua_pushcfunction(L, luaopen_debug);
  lua_pushstring(L, LUA_DBLIBNAME);
  lua_call(L, 1, 0);
#endif

  /* save normal pcall method */
  lua_pushlightuserdata(L, &clslua_pcall_reg_key);
  lua_getglobal(L, "pcall");
  lua_settable(L, LUA_REGISTRYINDEX);

  /* replace with our pcall method */
  lua_pushcfunction(L, clslua_pcall);
  lua_setglobal(L, "pcall");

  /*
   * Register cls functions (cls.log, etc...)
   */
  luaL_register(L, "cls", clslua_lib);

  /*
   * Register generic errno values under 'cls'
   */
  SET_INT_CONST(EPERM);
  SET_INT_CONST(ENOENT);
  SET_INT_CONST(ESRCH);
  SET_INT_CONST(EINTR);
  SET_INT_CONST(EIO);
  SET_INT_CONST(ENXIO);
  SET_INT_CONST(E2BIG);
  SET_INT_CONST(ENOEXEC);
  SET_INT_CONST(EBADF);
  SET_INT_CONST(ECHILD);
  SET_INT_CONST(EAGAIN);
  SET_INT_CONST(ENOMEM);
  SET_INT_CONST(EACCES);
  SET_INT_CONST(EFAULT);
  SET_INT_CONST(ENOTBLK);
  SET_INT_CONST(EBUSY);
  SET_INT_CONST(EEXIST);
  SET_INT_CONST(EXDEV);
  SET_INT_CONST(ENODEV);
  SET_INT_CONST(ENOTDIR);
  SET_INT_CONST(EISDIR);
  SET_INT_CONST(EINVAL);
  SET_INT_CONST(ENFILE);
  SET_INT_CONST(EMFILE);
  SET_INT_CONST(ENOTTY);
  SET_INT_CONST(ETXTBSY);
  SET_INT_CONST(EFBIG);
  SET_INT_CONST(ENOSPC);
  SET_INT_CONST(ESPIPE);
  SET_INT_CONST(EROFS);
  SET_INT_CONST(EMLINK);
  SET_INT_CONST(EPIPE);
  SET_INT_CONST(EDOM);
  SET_INT_CONST(ERANGE);
  lua_pop(L, 1);

  /* load bufferlist lib */
  lua_pushcfunction(L, luaopen_bufferlist);
  lua_pushstring(L, "bufferlist");
  lua_call(L, 1, 0);

  /* load msgpack lib */
  lua_pushcfunction(L, luaopen_cmsgpack);
  lua_pushstring(L, "cmsgpack");
  lua_call(L, 1, 0);
}

/*
 * Runs the script, and calls handler.
 */
static int clslua_eval(lua_State *L)
{
  struct clslua_hctx *ctx = __clslua_get_hctx(L);
  ctx->ret = -EIO; /* assume failure */

  /*
   * Load modules, errno value constants, and other environment goodies. Must
   * be done before loading/compiling the chunk.
   */
  clslua_setup_env(L);

  /*
   * Deserialize the input that contains the script, the name of the handler
   * to call, and the handler input.
   */
  lua_getglobal(L, "cmsgpack");
  lua_getfield(L, -1, "unpack");
  lua_pushlstring(L, ctx->inbl->c_str(), ctx->inbl->length());
  lua_call(L, 1, 1); /* puts the unpacked array on the stack */

  /* submitted lua script */
  lua_pushinteger(L, 1);
  lua_gettable(L, -2);
  const char *script = lua_tolstring(L, -1, NULL);

  /* name of handler function */
  lua_pushinteger(L, 2);
  lua_gettable(L, -3);
  const char *funcname = lua_tolstring(L, -1, NULL);

  /* handler input */
  lua_pushinteger(L, 3);
  lua_gettable(L, -4);
  size_t input_len;
  const char *input = lua_tolstring(L, -1, &input_len);

  /*
   * Create table to hold registered (valid) handlers.
   *
   * Must be done before running the script for the first time because the
   * script will immediately try to register one or more handlers using
   * cls.register(function), which depends on this table.
   */
  lua_pushlightuserdata(L, &clslua_registered_handle_reg_key);
  lua_newtable(L);
  lua_settable(L, LUA_REGISTRYINDEX);

  /* load and compile chunk */
  if (luaL_loadstring(L, script))
    return lua_error(L);

  /* execute chunk */
  lua_call(L, 0, 0);

  /* no error, but nothing left to do */
  if (!strlen(funcname)) {
    CLS_LOG(10, "no handler name provided");
    ctx->ret = 0; /* success */
    return 0;
  }

  lua_getglobal(L, funcname);
  if (lua_type(L, -1) != LUA_TFUNCTION) {
    CLS_ERR("error: unknown handler or not function: %s", funcname);
    ctx->ret = -EOPNOTSUPP;
    return 0;
  }

  /* throw error if function is not registered */
  clslua_check_registered_handler(L);

  /* setup the input/output bufferlists */
  bufferptr inbp(input, input_len);
  bufferlist inbl;
  inbl.push_back(inbp);
  clslua_pushbufferlist(L, &inbl);
  clslua_pushbufferlist(L, ctx->outbl);

  /*
   * Call the target Lua object class handler. If the call is successful then
   * we will examine the return value here and store it in the context. Errors
   * that occur are handled in the top-level eval() function.
   */
  int top = lua_gettop(L);
  lua_call(L, 2, LUA_MULTRET);

  /* store return value in context */
  if (!(lua_gettop(L) + 3 - top))
    lua_pushinteger(L, 0);
  ctx->ret = luaL_checkint(L, -1);

  return 0;
}

/*
 * Main handler. Proxies the Lua VM and the Lua-defined handler.
 */
static int eval(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  struct clslua_hctx ctx;
  lua_State *L = NULL;
  int ret = -EIO;

  /* stash context for use in Lua VM */
  ctx.hctx = &hctx;
  ctx.inbl = in;
  ctx.outbl = out;
  ctx.error.error = false;

  /* build lua vm state */
  L = luaL_newstate();
  if (!L) {
    CLS_ERR("error creating new Lua state");
    goto out;
  }

  /* panic handler for unhandled errors */
  lua_atpanic(L, &cls_lua_atpanic);

  if (setjmp(cls_lua_panic_jump) == 0) {

    /*
     * Stash the handler context in the register. It contains the objclass
     * method context, global error state, and the command and reply structs.
     */
    lua_pushlightuserdata(L, &clslua_hctx_reg_key);
    lua_pushlightuserdata(L, &ctx);
    lua_settable(L, LUA_REGISTRYINDEX);

    /* Process the input and run the script */
    lua_pushcfunction(L, clslua_eval);
    ret = lua_pcall(L, 0, 0, 0);

    /* Encountered an error? */
    if (ret) {
      struct clslua_err *err = clslua_checkerr(L);
      if (!err) {
        CLS_ERR("error: cls_lua state machine: unexpected error");
        assert(0);
      }

      /* Error origin a cls_cxx_* method? */
      if (err->error) {
        ret = err->ret; /* cls_cxx_* return value */

        /* Errors always abort. Fix up ret and log error */
        if (ret >= 0) {
          CLS_ERR("error: unexpected handler return value");
          ret = -EFAULT;
        }

      } else
        ret = -EIO; /* Generic error code */

      CLS_ERR("error: %s", lua_tostring(L, -1));

    } else {
      /*
       * No Lua error encountered while running the script, but the handler
       * may still have returned an error code (e.g. an errno value).
       */
      ret = ctx.ret;
    }

  } else {
    CLS_ERR("error: recovering from Lua panic");
    ret = -EFAULT;
  }

out:
  if (L)
    lua_close(L);
  return ret;
}

void __cls_init()
{
  CLS_LOG(20, "Loaded lua class!");

  cls_register("lua", &h_class);

  cls_register_cxx_method(h_class, "eval",
      CLS_METHOD_RD | CLS_METHOD_WR, eval, &h_eval);
}
