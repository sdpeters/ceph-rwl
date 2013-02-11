#include <errno.h>
#include <lua.hpp>
#include "include/types.h"
#include "include/rados/librados.h"
#include "gtest/gtest.h"
#include "test/librados/test.h"
#include "cls/lua/cls_lua_client.hpp"
#include "cls/lua/cls_lua.h"

/*
 * Auto-generated during build process. It includes the Lua unit test script
 * in the current directory, 'test_script.lua', serialized as a C string. See
 * src/Makefile.am for details on how that is generated.
 *
 * Makes available a static char* variable named 'cls_lua_test_script'.
 */
#include "test_script.h"
static string test_script;

/*
 * Test harness uses single pool for the entire test case, and generates
 * unique object names for each test.
 */
class ClsLua : public ::testing::Test {
  protected:
    static void SetUpTestCase() {
      pool_name = get_temp_pool_name();
      ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
      ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

      /* auto-generated from test_script.lua */
      test_script.assign(cls_lua_test_script);
    }

    static void TearDownTestCase() {
      ioctx.close();
      ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
    }

    virtual void SetUp() {
      /* Grab test names to build unique objects */
      const ::testing::TestInfo* const test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();

      /* Create unique string using test/testname/pid */
      std::stringstream ss_oid;
      ss_oid << test_info->test_case_name() << "_" <<
        test_info->name() << "_" << getpid();

      /* Unique object for test to use */
      oid = ss_oid.str();

      /*
       * Setup Lua env.
       *
       * We don't want to introduce a full dependency on the C++ MessagePack
       * library. However, it is a common case that a user will use MsgPack to
       * interface C++ and Lua. Rather than use the C++ library, we are going to
       * just re-use the Lua MsgPack library. However, it means we need to run
       * some Lua to actually pack and unpack the objects.
       *
       * It isn't the most robust test, but assuming the Lua MsgPack library
       * conforms to the standard (and this is the version used in Redis
       * production Lua support), it should be just fine.
       */
      L = luaL_newstate();
      ASSERT_TRUE(L != NULL);

      /* load msgpack lib */
      lua_pushcfunction(L, luaopen_cmsgpack);
      lua_pushstring(L, "cmsgpack");
      lua_call(L, 1, 0);
    }

    virtual void TearDown() {
      lua_close(L);
    }

    /*
     * Helper function. This functionality should eventually make its way into
     * a clslua client library of some sort.
     */
    int __clslua_exec(const string& oid, const string& script,
        librados::bufferlist *input = NULL,  const string& funcname = "")
    {
      bufferlist inbl;
      if (input)
        inbl = *input;

      return cls_lua_client::exec(ioctx, oid, script, funcname, inbl,
          reply_output);
    }

    int clslua_exec(const string& script, librados::bufferlist *input = NULL,
        const string& funcname = "")
    {
      return __clslua_exec(oid, script, input, funcname);
    }

    static librados::Rados rados;
    static librados::IoCtx ioctx;
    static string pool_name;
    static string test_script;

    string oid;
    bufferlist reply_output;

    lua_State *L;
};

librados::Rados ClsLua::rados;
librados::IoCtx ClsLua::ioctx;
string ClsLua::pool_name;
string ClsLua::test_script;

TEST_F(ClsLua, Write) {
  /* write some data into object */
  string written = "Hello World";
  bufferlist inbl;
  ::encode(written, inbl);
  ASSERT_EQ(0, clslua_exec(test_script, &inbl, "write"));

  /* have Lua read out of the object */
  uint64_t size;
  bufferlist outbl;
  ASSERT_EQ(0, ioctx.stat(oid, &size, NULL));
  ASSERT_EQ(size, (uint64_t)ioctx.read(oid, outbl, size, 0) );

  /* compare what Lua read to what we wrote */
  string read;
  ::decode(read, outbl);
  ASSERT_EQ(read, written);
}

TEST_F(ClsLua, SyntaxError) {
  ASSERT_EQ(-EIO, clslua_exec("-"));
}

TEST_F(ClsLua, EmptyScript) {
  ASSERT_EQ(0, clslua_exec(""));
}

TEST_F(ClsLua, RetVal) {
  /* handlers can return numeric values */
  ASSERT_EQ(1, clslua_exec(test_script, NULL, "rv_h1"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "rv_h0"));
  ASSERT_EQ(-1, clslua_exec(test_script, NULL, "rv_hn1"));
  ASSERT_EQ(1, clslua_exec(test_script, NULL, "rv_hs1"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "rv_hs0"));
  ASSERT_EQ(-1, clslua_exec(test_script, NULL, "rv_hsn1"));

  /* no return value is success */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "rv_h"));

  /* non-numeric return values are errors */
  ASSERT_EQ(-EIO, clslua_exec(test_script, NULL, "rv_hnil"));
  ASSERT_EQ(-EIO, clslua_exec(test_script, NULL, "rv_ht"));
  ASSERT_EQ(-EIO, clslua_exec(test_script, NULL, "rv_hstr"));
}

TEST_F(ClsLua, Create) {
  /* create works */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "create_c"));

  /* exclusive works */
  ASSERT_EQ(-EEXIST, clslua_exec(test_script, NULL, "create_c"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "create_cne"));
}

TEST_F(ClsLua, Pcall) {
  /* create and error works */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "pcall_c"));
  ASSERT_EQ(-EEXIST, clslua_exec(test_script, NULL, "pcall_c"));

  /* pcall masks the error */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "pcall_pc"));

  /* pcall lets us get the failed return value */
  ASSERT_EQ(-EEXIST, clslua_exec(test_script, NULL, "pcall_pcr"));

  /*
   * the first call in pcr2 will fail (check ret != 0), and the second pcall
   * should also fail (we check with a bogus return value to mask real
   * errors). This is also an important check for our error handling because
   * we need a case where two functions in the same handler fail to exercise
   * our internal error book keeping.
   */
  ASSERT_EQ(-9999, clslua_exec(test_script, NULL, "pcall_pcr2"));
}

TEST_F(ClsLua, Remove) {
  /* object doesn't exist */
  ASSERT_EQ(-ENOENT, clslua_exec(test_script, NULL, "remove_r"));

  /* can remove */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "remove_c"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "remove_r"));
  ASSERT_EQ(-ENOENT, clslua_exec(test_script, NULL, "remove_r"));
}

TEST_F(ClsLua, Stat) {
  /* build object and stat */
  char buf[1024];
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write_full(oid, bl));
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat(oid, &size, &mtime));

  /* test stat success */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "stat_ret"));

  /* unpack msgpack msg with Lua msgpack library */
  lua_getglobal(L, "cmsgpack");
  lua_getfield(L, -1, "unpack");
  lua_pushlstring(L, reply_output.c_str(), reply_output.length());
  ASSERT_EQ(0, lua_pcall(L, 1, 1, 0));
  /* array on top of stack now */

  /* check size -- first array element */
  lua_pushinteger(L, 1);
  lua_gettable(L, -2);
  ASSERT_EQ(lua_tointeger(L, -1), (int)size);
  lua_pop(L, 1);

  /* check mtime -- second array element */
  lua_pushinteger(L, 2);
  lua_gettable(L, -2);
  ASSERT_EQ(lua_tointeger(L, -1), (int)mtime);
  lua_pop(L, 1);

  /* test object dne */
  ASSERT_EQ(-ENOENT, __clslua_exec("dne", test_script, NULL, "stat_sdne"));

  /* can capture error with pcall */
  ASSERT_EQ(-ENOENT, __clslua_exec("dne", test_script, NULL, "stat_sdne_pcall"));
}

TEST_F(ClsLua, MapClear) {
  /* write some data into a key */
  string msg = "This is a test message";
  bufferlist val;
  val.append(msg.c_str(), msg.size());
  map<string, bufferlist> map;
  map["foo"] = val;
  ASSERT_EQ(0, ioctx.omap_set(oid, map));

  /* test we can get it back out */
  set<string> keys;
  keys.insert("foo");
  map.clear();
  ASSERT_EQ(0, (int)map.count("foo"));
  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys(oid, keys, &map));
  ASSERT_EQ(1, (int)map.count("foo"));

  /* now clear it */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_clear"));

  /* test that the map we get back is empty now */
  map.clear();
  ASSERT_EQ(0, (int)map.count("foo"));
  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys(oid, keys, &map));
  ASSERT_EQ(0, (int)map.count("foo"));
}

TEST_F(ClsLua, MapSetVal) {
  /* build some input value */
  bufferlist orig_val;
  ::encode("this is the original value yay", orig_val);

  /* have the lua script stuff the data into a map value */
  ASSERT_EQ(0, clslua_exec(test_script, &orig_val, "map_set_val"));

  /* grap the key now and compare to orig */
  map<string, bufferlist> out_map;
  set<string> out_keys;
  out_keys.insert("foo");
  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys(oid, out_keys, &out_map));
  bufferlist out_bl = out_map["foo"];
  string out_val;
  ::decode(out_val, out_bl);
  ASSERT_EQ(out_val, "this is the original value yay");
}

TEST_F(ClsLua, MapGetVal) {
  /* write some data into a key */
  string msg = "This is a test message";
  bufferlist orig_val;
  orig_val.append(msg.c_str(), msg.size());
  map<string, bufferlist> orig_map;
  orig_map["foo"] = orig_val;
  ASSERT_EQ(0, ioctx.omap_set(oid, orig_map));

  /* now compare to what we put it */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_get_val_foo"));

  /* check return */
  string ret_val;
  ret_val.assign(reply_output.c_str(), reply_output.length());
  ASSERT_EQ(ret_val, msg);

  /* error case */
  ASSERT_EQ(-ENOENT, clslua_exec(test_script, NULL, "map_get_val_dne"));
}

TEST_F(ClsLua, Read) {
  /* put data into object */
  string msg = "This is a test message";
  bufferlist bl;
  bl.append(msg.c_str(), msg.size());
  ASSERT_EQ(0, ioctx.write_full(oid, bl));

  /* get lua to read it and send it back */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "read"));

  /* check return */
  string ret_val;
  ret_val.assign(reply_output.c_str(), reply_output.length());
  ASSERT_EQ(ret_val, msg);
}

TEST_F(ClsLua, Log) {
  ASSERT_EQ(0, clslua_exec("cls.log()"));
  ASSERT_EQ(0, clslua_exec("s = cls.log(); cls.log(s);"));
  ASSERT_EQ(0, clslua_exec("cls.log(1)"));
  ASSERT_EQ(0, clslua_exec("cls.log(-1)"));
  ASSERT_EQ(0, clslua_exec("cls.log('x')"));
  ASSERT_EQ(0, clslua_exec("cls.log(0, 0)"));
  ASSERT_EQ(0, clslua_exec("cls.log(1, 1)"));
  ASSERT_EQ(0, clslua_exec("cls.log(-10, -10)"));
  ASSERT_EQ(0, clslua_exec("cls.log('x', 'y')"));
  ASSERT_EQ(0, clslua_exec("cls.log(1, 'one')"));
  ASSERT_EQ(0, clslua_exec("cls.log(1, 'one', 'two')"));
  ASSERT_EQ(0, clslua_exec("cls.log('one', 'two', 'three')"));
  ASSERT_EQ(0, clslua_exec("s = cls.log('one', 'two', 'three'); cls.log(s);"));
}

TEST_F(ClsLua, BufferlistEquality) {
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_eq_empty_equal"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_eq_empty_selfequal"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_eq_selfequal"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_eq_equal"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_eq_notequal"));
}

TEST_F(ClsLua, RunError) {
  ASSERT_EQ(-EIO, clslua_exec(test_script, NULL, "runerr_c"));
}

TEST_F(ClsLua, HandleNotFunc) {
  string script = "x = 1;";
  ASSERT_EQ(-EOPNOTSUPP, clslua_exec(script, NULL, "x"));
}

TEST_F(ClsLua, Register) {
  /* normal cases: register and maybe call the handler */
  string script = "function h() end; cls.register(h);";
  ASSERT_EQ(0, clslua_exec(script, NULL, ""));
  ASSERT_EQ(0, clslua_exec(script, NULL, "h"));

  /* can register and call multiple handlers */
  script = "function h1() end; function h2() end;"
    "cls.register(h1); cls.register(h2);";
  ASSERT_EQ(0, clslua_exec(script, NULL, ""));
  ASSERT_EQ(0, clslua_exec(script, NULL, "h1"));
  ASSERT_EQ(0, clslua_exec(script, NULL, "h2"));

  /* normal cases: register before function is defined */
  script = "cls.register(h); function h() end;";
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, ""));
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, "h"));

  /* cannot call handler that isn't registered */
  script = "function h() end;";
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, "h"));

  /* handler doesn't exist */
  script = "cls.register(lalala);";
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, ""));

  /* handler isn't a function */
  script = "cls.register('some string');";
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, ""));

  /* cannot register handler multiple times */
  script = "function h() end; cls.register(h); cls.register(h);";
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, ""));
}

TEST_F(ClsLua, BufferlistCompare) {
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_lt"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_le"));
}

TEST_F(ClsLua, BufferlistConcat) {
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_concat_eq"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_concat_ne"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_concat_immut"));
}

TEST_F(ClsLua, GetXattr) {
  bufferlist bl;
  bl.append("blahblahblahblahblah");
  ASSERT_EQ(0, ioctx.setxattr(oid, "fooz", bl));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "getxattr"));
  ASSERT_TRUE(reply_output == bl);
}

TEST_F(ClsLua, SetXattr) {
  bufferlist inbl;
  inbl.append("blahblahblahblahblah");
  ASSERT_EQ(0, clslua_exec(test_script, &inbl, "setxattr"));
  bufferlist outbl;
  ASSERT_EQ((int)inbl.length(), ioctx.getxattr(oid, "fooz2", outbl));
  ASSERT_TRUE(outbl == inbl);
}
