#ifndef __LIBRADOSGW_HPP
#define __LIBRADOSGW_HPP

#include <string>
#include <map>

#include "rados/librados.hpp"


namespace libradosgw {

  struct RefCountedObject {
    int nref;
    RefCountedObject() : nref(1) {}
    virtual ~RefCountedObject() {}
  
    RefCountedObject *get() {
      ++nref;
      return this;
    }
    void put() {
      if (--nref == 0)
        delete this;
    }
  };


  template <class T>
  class ObjRef {
    RefCountedObject *obj;
  public:
    ObjRef(RefCountedObject *o = NULL) : obj(o) {}

    ObjRef(ObjRef<T>& src) {
    obj = src;
    if (obj)
      obj->get();
    }

    ~ObjRef() {
      if (obj)
	obj->put();
    }

    ObjRef<T>& operator=(ObjRef<T> &src) {
      if (this == &src)
        return *this;

      if (src.obj)
        src.obj->get();

      if (obj)
	obj->put();
 
      obj = src.obj;
      return *this;
    }

    RefCountedObject *operator=(RefCountedObject *o) {
      if (obj)
	obj->put();

      obj = o;
      return obj;
    }

    T *operator->() {
      return (T *)obj;
    }
  };


  using std::string;
  using librados::Rados;
  using ceph::bufferlist;

  class StoreImpl;
  class AccountImpl;
  class UserImpl;

  enum RGWPerm {
    PERM_READ      = 0x01,
    PERM_WRITE     = 0x02,
    PERM_READ_ACP  = 0x04,
    PERM_WRITE_ACP = 0x08,
  };

  enum RGWGroup {
    GROUP_NOT_GROUP     = 0,
    GROUP_ANONYMOUS     = 1,
    GROUP_AUTHENTICATED = 2,
  };

  struct AccessKey {
    string id;
    string key;
    string subuser;
  };

  struct SubUser {
    string name;
    uint32_t perm_mask;
  };

  class ImplContainer {
  protected:
    void *impl;

  public:
    ImplContainer() : impl(NULL) {}
    virtual ~ImplContainer();

    ImplContainer& operator=(ImplContainer& c);
  };

  struct User {
    int group;
    string uid;
    string display_name;
    string email;
    uint64_t auid;

    bool is_anonymous() { return (group & GROUP_ANONYMOUS) != 0; }
  };

  struct ACLs {
    User owner;
    std::map<User, int> acl_map;
  };


  struct Attrs {
    std::map<string, ceph::bufferlist> meta_map;
  };

  struct BucketInfo {
    string name;
    time_t creation_time;
    uint64_t size;
    uint64_t num_objects;

    string owner_display_name;
    string owner_id;
  };


  struct ObjectInfo {
    string name;
    uint64_t len;
    time_t mtime;
    string owner;
    string owner_display_name;
    string etag;
    string content_type;
    Attrs attrs;
  };

  class Object {
    int read(bufferlist& bl, uint64_t ofs, uint64_t len);
    int write(bufferlist& bl);
    int cancel();
    int close();
    
    int get_attrs(Attrs& attrs);
    int set_attrs(Attrs& attrs);

    int set_acls(ACLs& acls);
    int get_acls(ACLs& acls);

    ObjectInfo& get_info();
  };

  class MultipartUploadIterator : public std::iterator <std::forward_iterator_tag, std::string>  {
    static const MultipartUploadIterator __EndIterator;
    MultipartUploadIterator& operator++();
    const ObjectInfo& operator*() const;
  };
  
  class MultipartObject {
  public:
    MultipartUploadIterator parts_begin();
    const MultipartUploadIterator& parts_end();

    Object *get_part(int index);
    int cancel();
    int complete();
  };

  class ManifestObject {
  public:
    int cancel();
    int complete();
  };

  class BucketIterator : public std::iterator <std::forward_iterator_tag, std::string>  {
    static const BucketIterator __EndIterator;
    BucketIterator& operator++();
    const ObjectInfo& operator*() const;
  };

  class BucketMultipartIterator : public std::iterator <std::forward_iterator_tag, std::string>  {
    static const BucketIterator __EndIterator;
    BucketMultipartIterator& operator++();
    MultipartObject operator*() const;
  };

  class Bucket {
  public:
    Bucket();

    BucketIterator objects_begin(const char *prefix = NULL, const char *delim = NULL);
    BucketIterator objects_find(char *marker, int max = 0, const char *prefix = NULL, const char *delim = NULL);

    BucketMultipartIterator multipart_uploads_begin();
    const BucketMultipartIterator& multipart_uploads_end();

    const BucketIterator& objects_end();
    
    int open(string& name, Object& obj);
    int stat(string&name, ObjectInfo& info);
    int create(string& name, Object& obj, ACLs *acls = NULL);
    int remove(string& name);

    int create_multipart(string& name, MultipartObject& obj, ACLs *acls = NULL);
    int create_manifest(string& name, ManifestObject& obj, ACLs *acls = NULL);

    int cancel_multipart_upload(MultipartObject& obj);

    int get_acls(ACLs& acls);
    int set_acls(ACLs& acls);

    int get_attrs(Attrs& attrs);
  };

  class AccountIterator : public std::iterator <std::forward_iterator_tag, std::string>  {
    static const AccountIterator __EndIterator;
    AccountIterator& operator++();
    const BucketInfo& operator*() const;
  };

  class Account {
    friend class AccountImpl;
    friend class StoreImpl;

  protected:
    ObjRef<AccountImpl> impl;

    User user;
    std::map<string, AccessKey> access_keys;
    std::map<string, AccessKey> swift_keys;
    std::map<string, SubUser> subusers;

    uint64_t auid;
    bool suspended;
  public:
    Account();
    ~Account();

    AccountIterator buckets_begin();
    const AccountIterator& buckets_end();

    int remove_bucket(string& name);
    int get_bucket(string& name, Bucket& bucket);
    int create_bucket(string& name, ACLs *acls = NULL);

    std::map<string, AccessKey>& get_access_keys() { return access_keys; }
    std::map<string, AccessKey> get_swift_keys() { return swift_keys; }
    std::map<string, SubUser> get_subusers() { return subusers; }
  };


  class Store {
  protected:
    ObjRef<StoreImpl> impl;
  public:
    Store() : impl(NULL) {}

    int init(CephContext *cct);
    void shutdown();

    int account_by_name(string& name, Account& account);
    int account_by_email(string& email, Account& account);
    int account_by_access_key(string& access_key, Account& account);
    int account_by_subuser(string& subuser, Account& account);

    int user_by_name(string& name, User& user);
    int user_by_email(string& email, User& user);
    int user_by_access_key(string& access_key, User& user);
    int user_by_subuser(string& subuser, User& user);
  };
}


#endif
