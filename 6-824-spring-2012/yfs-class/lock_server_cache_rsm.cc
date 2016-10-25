// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) 
  : rsm (_rsm)
{
}

int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id, 
             lock_protocol::xid_t xid, int &)
{
	printf("acquire(%llu, %s, %llu)\n", lid, id.c_str(), xid);
	std::pair<lock_protocol::xid_t, lock_protocol::status>& pa = acq_reply[id];
	lock_protocol::status ret = lock_protocol::OK;
	if (pa.first < xid) {
		pa.first = xid;
		std::list<std::string>& ac = lock_acquirers[lid];
		ac.push_back(id);
		if (ac.size() > 1) {
			pa.second = lock_protocol::RETRY;
			if (ac.size() == 2) {
				printf("lock %llu is hold by %s, revoking\n", lid, ac.front().c_str());
				std::string acquirer_id = ac.front();
				if (!rxids.count(acquirer_id)) {
					rxids[acquirer_id] = 1;
				}
				lock_protocol::xid_t& rxid = rxids[acquirer_id];
				last_revoke = rcall_t(rlock_protocol::revoke, xid, rxid++, lid, acquirer_id);
			}
			printf("lock %llu is still hold, %s retries\n", lid, id.c_str());
		} else {
			pa.second = lock_protocol::OK;
			printf("lock %llu is free, %s become owner\n", lid, id.c_str());
		}
	}
	if (xid == pa.first) {
		ret = pa.second;	
		if (last_revoke.xid == xid && rsm->amiprimary()) {
			rcall(last_revoke);
		}
	} 
  return ret;
}

int 
lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
         lock_protocol::xid_t xid, int &r)
{
	printf("release(%llu, %s, %llu)\n", lid, id.c_str(), xid);
	lock_protocol::xid_t& reply_xid = rel_reply[id];
	lock_protocol::status ret = lock_protocol::OK;
	if (reply_xid < xid) {
		reply_xid = xid;
		std::list<std::string>& acquirers = lock_acquirers[lid];
		acquirers.pop_front();
		if (!acquirers.empty()) {
			std::string acquirer_id = acquirers.front();
			handle h(acquirer_id);
			rpcc* cl = h.safebind();
			VERIFY(cl != NULL);
			if (!rxids.count(acquirer_id)) {
				rxids[acquirer_id] = 1;
			}
			lock_protocol::xid_t& rxid = rxids[acquirer_id];
			last_retry = rcall_t(rlock_protocol::retry, xid, rxid++, lid, acquirer_id);
			if (acquirers.size() > 1) {
				last_revoke = rcall_t(rlock_protocol::revoke, xid, rxid++, lid, acquirer_id);
			}
		}
	}
	if (reply_xid == xid && rsm->amiprimary()) {
		if (last_retry.xid == xid) {
			rcall(last_retry);
		}
		if (last_revoke.xid == xid) {
			rcall(last_revoke);
		}
	}
  return ret;
}

std::string
lock_server_cache_rsm::marshal_state()
{
	marshall m;
	m << acq_reply << rel_reply;
	m << rxids;
	m << lock_acquirers;
	m << last_retry.num << last_retry.xid << last_retry.rxid << last_retry.lid << last_retry.id;
	m << last_revoke.num << last_revoke.xid << last_revoke.rxid << last_revoke.lid << last_revoke.id;
  return m.str();
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
	const std::string s = state;
	unmarshall u(s);	
	u >> acq_reply >> rel_reply;
	u >> rxids;
	u >> lock_acquirers;
	u >> last_retry.num >> last_retry.xid >> last_retry.rxid >> last_retry.lid >> last_retry.id;
	u >> last_revoke.num >> last_revoke.xid >> last_revoke.rxid >> last_revoke.lid >> last_revoke.id;
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  printf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

void lock_server_cache_rsm::rcall(const rcall_t& rc) {
	handle h(rc.id);
	rpcc* cl = h.safebind();
	VERIFY(cl != NULL);
	int r;
	int ret = cl->call(rc.num, rc.lid, rc.rxid, r);
	VERIFY(ret == rlock_protocol::OK);
}
