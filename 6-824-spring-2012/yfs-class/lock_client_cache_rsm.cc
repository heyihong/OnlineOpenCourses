// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache_rsm.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

#include "rsm_client.h"

static void *
releasethread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache_rsm::last_port = 0;

lock_client_cache_rsm::lock_client_cache_rsm(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu), releases(0)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache_rsm::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache_rsm::retry_handler);
  acq_xid = rel_xid = 1;
	reply = 0;
  // You fill this in Step Two, Lab 7
  // - Create rsmc, and use the object to do RPC 
  //   calls instead of the rpcc object of lock_client
	rsmc = new rsm_client(xdst);
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  VERIFY (r == 0);
	pthread_mutex_init(&mu, NULL);
	pthread_cond_init(&hdl_cond, NULL);
}

lock_client_cache_rsm::~lock_client_cache_rsm() {	
	pthread_join(th, NULL);
	delete rsmc;
	pthread_cond_destroy(&hdl_cond);
	pthread_mutex_destroy(&mu);
}

void
lock_client_cache_rsm::releaser()
{

  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.
	lock_protocol::lockid_t lid;
	int r;
	int ret;
	while (true) {
		releases.deq(&lid);
		if (lu != NULL) {
			lu->dorelease(lid);
		}
		ret = rsmc->call(lock_protocol::release, lid, id, rel_xid, r);		  	
		rel_xid++;
		VERIFY(ret == lock_protocol::OK);
	}
}


lock_protocol::status
lock_client_cache_rsm::acquire(lock_protocol::lockid_t lid)
{
  int ret;
	int r;
	ScopedLock ml(&mu);
	printf("acquire(%llu)\n", lid);
	lockent& ent = locks[lid];
	while (true) {
		if (ent.status == NONE) {
			printf("lock %llu is not in cache, acquiring\n", lid);
			ent.status = ACQUIRING;
			pthread_mutex_unlock(&mu);		
			ret = rsmc->call(lock_protocol::acquire, lid, id, acq_xid, r);
			acq_xid++;
			VERIFY(ret == lock_protocol::OK || ret == lock_protocol::RETRY);
			pthread_mutex_lock(&mu);
			if (ret == lock_protocol::OK) {
				ent.get_lock = true;
			}
		}
		if (ent.status == ACQUIRING && ent.get_lock) {
			ent.status = LOCKED;
			ent.get_lock = false;
			if (ent.revoke) {
				ent.revoke = false;
				ent.status = RELEASING;
			}
			break;
		} else if (ent.status == FREE) {
			ent.status = LOCKED;
			break;
		}
		pthread_cond_wait(&hdl_cond, &mu);
	}
  return ret;
}

lock_protocol::status
lock_client_cache_rsm::release(lock_protocol::lockid_t lid)
{
  ScopedLock ml(&mu);	
	printf("release(%llu)\n", lid);
	lockent& ent = locks[lid];
	VERIFY(ent.status == LOCKED || ent.status == RELEASING);
	if (ent.status == LOCKED) {
		printf("lock %llu is released\n", lid);
		ent.status = FREE;
	} else {
		printf("release lock %llu to lock server\n", lid);
		ent.status = NONE;
		releases.enq(lid);
	}
  return lock_protocol::OK;
}


rlock_protocol::status
lock_client_cache_rsm::revoke_handler(lock_protocol::lockid_t lid, 
			          lock_protocol::xid_t xid, int &)
{
	ScopedLock ml(&mu);
	if (reply < xid) {
		printf("revoke_handler(%llu, %llu)\n", lid, xid);
		reply = xid;
		lockent& ent = locks[lid];
		VERIFY(ent.status == ACQUIRING || ent.status == FREE || ent.status == LOCKED);
		if (ent.status == ACQUIRING) {
			ent.revoke = true;
		} else if (ent.status == FREE) {
			ent.status = NONE;
			releases.enq(lid);
			pthread_cond_signal(&hdl_cond);
		} else {
			ent.status = RELEASING;
		}
	}
  return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache_rsm::retry_handler(lock_protocol::lockid_t lid, 
			         lock_protocol::xid_t xid, int &)
{
	ScopedLock ml(&mu);
	if (reply < xid) {
		printf("retry_handler(%llu, %llu)\n", lid, xid);
		reply = xid;
		lockent& ent = locks[lid];
		VERIFY(ent.status == ACQUIRING);
		ent.get_lock = true;
		pthread_cond_signal(&hdl_cond);
	}
  return lock_protocol::OK;
}


