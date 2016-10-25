#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <string>

#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "rsm.h"

class lock_server_cache_rsm : public rsm_state_transfer {
 private:
  int nacquire;
  class rsm *rsm;
	std::map<std::string, std::pair<lock_protocol::xid_t, lock_protocol::status> > acq_reply;
	std::map<std::string, lock_protocol::xid_t> rel_reply;
	std::map<std::string, lock_protocol::xid_t> rxids;
	std::map<lock_protocol::lockid_t, std::list<std::string> > lock_acquirers;
	struct rcall_t {
		int num;
		lock_protocol::xid_t xid;
		lock_protocol::xid_t rxid;
		lock_protocol::lockid_t lid;
		std::string id;
		rcall_t() {}
		rcall_t(int _num, lock_protocol::xid_t _xid, lock_protocol::xid_t _rxid, lock_protocol::lockid_t _lid, std::string _id): num(_num), xid(_xid), rxid(_rxid), lid(_lid), id(_id) {}
	};
	rcall_t last_retry;
	rcall_t last_revoke;
	void rcall(const rcall_t& c);
 public:
  lock_server_cache_rsm(class rsm *rsm = 0);
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  std::string marshal_state();
  void unmarshal_state(std::string state);
  int acquire(lock_protocol::lockid_t, std::string id, 
	      lock_protocol::xid_t, int &);
  int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
	      int &);
};

#endif
