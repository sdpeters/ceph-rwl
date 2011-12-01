#ifndef __LIBRADOSGW_HPP
#define __LIBRADOSGW_HPP

namespace libradosgw {

  using ceph::bufferlist;

  enum RGWPerm {
    PERM_READ      = 0x01;
    PERM_WRITE     = 0x02;
    PERM_READ_ACP  = 0x04;
    PERM_WRITE_ACP = 0x08;
  };

  enum RGWGroup {
    GROUP_NOT_GROUP     = 0;
    GROUP_ANONYMOUS     = 1;
    GROUP_AUTHENTICATED = 2;
  };

  class User {
    int group;
    string name;
    string display_name;
    string email;
  public:
    
    void set_group(int g) { group = g; }

    bool is_anonymous() { return (group & GROUP_ANONYMOUS) != 0; }
    int get_group() { return group; }
    const string& get_name() { return name; }
  };

  struct ACLs {
    User owner;
    map<User, int> acl_map;
  };


  struct Attrs {
    map<string, ceph::bufferlist> meta_map;
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

    BucketIterator objects_begin(const char *delim = NULL, const char *marker = NULL);
    BucketIterator objects_find(char *marker, int max = 0, const char *delim = NULL, const char *marker = NULL);

    BucketMultipartIterator multipart_uploads_begin();
    const BucketMultipartIterator& multipart_uploads_end();

    const BucketIterator& objects_end();
    
    int open(string& name, Object& obj);
    int stat(string&name, ObjectInfo& info);
    int create(string& name, Object& obj, ACLS *acls = NULL);
    int remove(string& name);

    int create_multipart(string& name, MultipartObject& obj, ACLS *acls = NULL);
    int create_manifest(string& name, ManifestObject& obj, ACLS *acls = NULL);

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
    AccountIterator buckets_begin();
    const AccountIterator& buckets_end();

    int remove_bucket(string& name);
    int get_bucket(string& name, Bucket& bucket);
    int create_bucket(string& name, ACLs *acls = NULL);
  };


  class Store {
  public:
    Store();

    int init(librados::Rados *r);
    void shutdown();

    Account get_account(string& name);

    User user_by_name(string& name);
    User user_by_email(string& email);
    User user_by_access_key(string& access_key);
  };
}


#endif
