#include <errno.h>
#include <string>
#include <vector>
#include "include/encoding.h"
#include "include/rados.h"
#include "include/rados/librados.h"
#include "include/types.h"
#include "cls_lua.h"
#include "cls_lua_client.hpp"

using std::string;
using std::vector;
using librados::IoCtx;
using librados::bufferlist;

namespace cls_lua_client {

  int exec(IoCtx& ioctx, const string& oid, const string& script,
      const string& handler, bufferlist& input, bufferlist& output)
  {
    int ret;

    lua_State *L = luaL_newstate();
    assert(L);

    /* load msgpack lib */
    lua_pushcfunction(L, luaopen_cmsgpack);
    lua_pushstring(L, "cmsgpack");
    lua_call(L, 1, 0);

    lua_getglobal(L, "cmsgpack");
    lua_getfield(L, -1, "pack");

    lua_newtable(L);

    /* set script at index = 1 */
    lua_pushinteger(L, 1);
    lua_pushstring(L, script.c_str());
    lua_settable(L, -3);

    /* set handler name at index = 2 */
    lua_pushinteger(L, 2);
    lua_pushstring(L, handler.c_str());
    lua_settable(L, -3);

    /* set handler input at index = 3 */
    lua_pushinteger(L, 3);
    lua_pushlstring(L, input.c_str(), input.length());
    lua_settable(L, -3);

    /* call cmsgpack.pack */
    lua_call(L, 1, 1);

    /* setup the input / output */
    size_t input_len;
    const char *input_str = lua_tolstring(L, -1, &input_len);

    bufferptr inbp(input_str, input_len);
    bufferlist inbl, outbl;
    inbl.push_back(inbp);

    /*
     * TODO: we need to encapsulate the return value as well. for example,
     * -ENOTSUPP is returned if the class is not found, but we also return
     * -ENOTSUPP if a handler isn't found. In the later case we still get a
     * valid reply, in the former not so much.
     */
    ret = ioctx.exec(oid, "lua", "eval", inbl, output);

    return ret;
  }

}
