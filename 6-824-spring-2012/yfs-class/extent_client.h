// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "rpc.h"

class extent_client {
 private:
  rpcc *cl;
  static const int BUF_BIT = 1 << 0;
  static const int ATTR_BIT = 1 << 1;
  static const int DIRTY_BIT = 1 << 2;
  static const int DELETED_BIT = 1 << 3;
  struct extent_ent {
	  extent_ent() {
		  buf = "";
		  bits = 0;
	  }
	  std::string buf;
	  extent_protocol::attr attr;
	  int bits;
  };
  static std::map<extent_protocol::extentid_t, extent_client::extent_ent> extents;
  static pthread_mutex_t mu;

 public:
  extent_client(std::string dst);
  virtual ~extent_client();
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			      std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  void flush(extent_protocol::extentid_t eid);
};

#endif 

