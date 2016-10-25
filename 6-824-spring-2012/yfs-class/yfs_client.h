#ifndef yfs_client_h
#define yfs_client_h

#include <string>
#include <list>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include "lock_client_cache.h"

#include "lock_protocol.h"

class yfs_client {
  extent_client *ec;
  lock_client *lc;
  lock_release_user * lu;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::vector<dirent> string2dirents(const std::string&);
  static std::string dirents2string(const std::vector<dirent>&);
  static std::vector<dirent>::iterator finddirent(std::vector<dirent>&, const std::string&);
	  
  class lock_release_extent: public lock_release_user 
  {
    public:
	  void dorelease(lock_protocol::lockid_t);
	  lock_release_extent(extent_client*);
	private:
	  extent_client* client;
  };

 public:

  yfs_client(std::string, std::string);
  virtual ~yfs_client();

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int create(inum, const dirent&);

  int getdirents(inum, std::vector<dirent>& dirents);

  int getbuf(inum, size_t, size_t, std::string&);
  int setbuf(inum, size_t, const std::string&, bool);

  int rmfile(inum, const std::string&);
};

#endif 
