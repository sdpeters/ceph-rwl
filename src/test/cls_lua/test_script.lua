--
-- This Lua script file contains all of the handlers used in the cls_lua unit
-- tests (src/test/cls_lua/test_cls_lua.cc). Each section header corresponds
-- to the ClsLua.XYZ test.
--

--
-- Read
--
function read(input, output)
  size = cls.stat()
  bl = cls.read(0, size)
  output:append(bl:str())
end

cls.register(read)

--
-- Write
--
function write(input, output)
  cls.write(0, #input, input)
end

cls.register(write)

--
-- MsgPack
--
function msgpack(input, output)
  data = input:str()
  obj = cmsgpack.unpack(data)
  cls.log(obj)
  ret = cmsgpack.pack(obj)
  cls.log(ret)
  output:append(ret)
end

cls.register(msgpack)

--
-- MapGetVal
--
function map_get_val_foo(input, output)
  bl = cls.map_get_val('foo')
  output:append(bl:str())
end

function map_get_val_dne()
  bl = cls.map_get_val('dne')
end

cls.register(map_get_val_foo)
cls.register(map_get_val_dne)

--
-- Stat
--
function stat_ret(input, output)
  size, mtime = cls.stat()
  data = {}
  data[1] = size
  data[2] = mtime
  ret = cmsgpack.pack(data)
  output:append(ret)
end

function stat_sdne()
  size, mtime = cls.stat()
end

function stat_sdne_pcall()
  ok, ret, size, mtime = pcall(cls.stat, o)
  assert(ok == false)
  assert(ret == -cls.ENOENT)
  return ret
end

cls.register(stat_ret)
cls.register(stat_sdne)
cls.register(stat_sdne_pcall)

--
-- RetVal
--
function rv_h() end
function rv_h1() return 1; end
function rv_h0() return 0; end
function rv_hn1() return -1; end
function rv_hs1() return '1'; end
function rv_hs0() return '0'; end
function rv_hsn1() return '-1'; end
function rv_hnil() return nil; end
function rv_ht() return {}; end
function rv_hstr() return 'asdf'; end

cls.register(rv_h)
cls.register(rv_h1)
cls.register(rv_h0)
cls.register(rv_hn1)
cls.register(rv_hs1)
cls.register(rv_hs0)
cls.register(rv_hsn1)
cls.register(rv_hnil)
cls.register(rv_ht)
cls.register(rv_hstr)

--
-- Create
--
function create_c() cls.create(true); end
function create_cne() cls.create(false); end

cls.register(create_c)
cls.register(create_cne)

--
-- Pcall
--
function pcall_c() cls.create(true); end

function pcall_pc()
  ok, ret = pcall(cls.create, true)
  assert(ok == false)
  assert(ret == -cls.EEXIST)
end

function pcall_pcr()
  ok, ret = pcall(cls.create, true)
  assert(ok == false)
  assert(ret == -cls.EEXIST)
  return ret
end

function pcall_pcr2()
  ok, ret = pcall(cls.create, true)
  assert(ok == false)
  assert(ret == -cls.EEXIST)
  ok, ret = pcall(cls.create, true)
  assert(ok == false)
  assert(ret == -cls.EEXIST)
  return -9999
end

cls.register(pcall_c)
cls.register(pcall_pc)
cls.register(pcall_pcr)
cls.register(pcall_pcr2)

--
-- Remove
--
function remove_c() cls.create(true); end
function remove_r() cls.remove(); end

cls.register(remove_c)
cls.register(remove_r)

--
-- MapSetVal
--
function map_set_val(input, output)
  cls.map_set_val('foo', input)
end

cls.register(map_set_val)

--
-- MapClear
--
function map_clear()
  cls.map_clear()
end

cls.register(map_clear)

--
-- BufferlistEquality
--
function bl_eq_empty_equal(input, output)
  bl1 = bufferlist.new()
  bl2 = bufferlist.new()
  assert(bl1 == bl2)
end

function bl_eq_empty_selfequal()
  bl1 = bufferlist.new()
  assert(bl1 == bl1)
end

function bl_eq_selfequal()
  bl1 = bufferlist.new()
  bl1:append('asdf')
  assert(bl1 == bl1)
end

function bl_eq_equal()
  bl1 = bufferlist.new()
  bl2 = bufferlist.new()
  bl1:append('abc')
  bl2:append('abc')
  assert(bl1 == bl2)
end

function bl_eq_notequal()
  bl1 = bufferlist.new()
  bl2 = bufferlist.new()
  bl1:append('abc')
  bl2:append('abcd')
  assert(bl1 ~= bl2)
end

cls.register(bl_eq_empty_equal)
cls.register(bl_eq_empty_selfequal)
cls.register(bl_eq_selfequal)
cls.register(bl_eq_equal)
cls.register(bl_eq_notequal)

--
-- Bufferlist Compare
--
function bl_lt()
  local a = bufferlist.new()
  local b = bufferlist.new()
  a:append('A')
  b:append('B')
  assert(a < b)
end

function bl_le()
  local a = bufferlist.new()
  local b = bufferlist.new()
  a:append('A')
  b:append('B')
  assert(a <= b)
end

cls.register(bl_lt)
cls.register(bl_le)

--
-- Bufferlist concat
--
function bl_concat_eq()
  local a = bufferlist.new()
  local b = bufferlist.new()
  local ab = bufferlist.new()
  a:append('A')
  b:append('B')
  ab:append('AB')
  assert(a .. b == ab)
end

function bl_concat_ne()
  local a = bufferlist.new()
  local b = bufferlist.new()
  local ab = bufferlist.new()
  a:append('A')
  b:append('B')
  ab:append('AB')
  assert(b .. a ~= ab)
end

function bl_concat_immut()
  local a = bufferlist.new()
  local b = bufferlist.new()
  local ab = bufferlist.new()
  a:append('A')
  b:append('B')
  ab:append('AB')
  x = a .. b
  assert(x == ab)
  b:append('C')
  assert(x == ab)
  local bc = bufferlist.new()
  bc:append('BC')
  assert(b == bc)
end

cls.register(bl_concat_eq)
cls.register(bl_concat_ne)
cls.register(bl_concat_immut)

--
-- RunError
--
function runerr_a()
  error('WTF')
end

function runerr_b()
  runerr_a()
end

function runerr_c()
  runerr_b()
end

-- only runerr_c is called
cls.register(runerr_c)

--
-- GetXattr
--
function getxattr(input, output)
  bl = cls.getxattr("fooz")
  output:append(bl:str())
end

cls.register(getxattr)

--
-- SetXattr
--
function setxattr(input, output)
  cls.setxattr("fooz2", input)
end

cls.register(setxattr)
