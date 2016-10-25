// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

std::map<lock_protocol::lockid_t, lock_client_cache::lockent> lock_client_cache::locks;
pthread_mutex_t lock_client_cache::lock_mu = PTHREAD_MUTEX_INITIALIZER;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlsrpc->port();
  id = host.str();
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&lock_mu);
  printf("acquire(%llu)\n", lid);
  lockent& ent = locks[lid];
  int r;
  int ret;
  while (true) {
	  if (ent.status == NONE) {
		printf("lock %llu is not in cache, acquiring\n", lid);
	  	ent.status = ACQUIRING;
	    pthread_mutex_unlock(&lock_mu);	  
	    ret = cl->call(lock_protocol::acquire, lid, id, r);	
		VERIFY(ret == lock_protocol::OK || ret == lock_protocol::RETRY);
		pthread_mutex_lock(&lock_mu);
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
	  pthread_cond_wait(&ent.cond, &lock_mu);
  }
  pthread_mutex_unlock(&lock_mu);
  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  int r;
  pthread_mutex_lock(&lock_mu);
  printf("release(%llu)\n", lid);
  lockent& ent = locks[lid];
  VERIFY(ent.status == LOCKED || ent.status == RELEASING);
  if (ent.status == LOCKED) {
	printf("lock %llu is released\n", lid);
	ent.status = FREE;
  } else {
	printf("release lock %llu to lock server\n", lid);
	ent.status = NONE;
    pthread_mutex_unlock(&lock_mu);
	if (lu != NULL) {
	  lu->dorelease(lid);
	}
	ret = cl->call(lock_protocol::release, lid, id, r);	
	VERIFY(ret == lock_protocol::OK);
    pthread_mutex_lock(&lock_mu);
  }
  // signal one waiting process so that it can either get free lock or acquire lock from server
  pthread_cond_signal(&ent.cond);
  pthread_mutex_unlock(&lock_mu);
  return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, int &)
{
  int ret = rlock_protocol::OK;
  int r;
  pthread_mutex_lock(&lock_mu);
  printf("revoke_handler(%llu)\n", lid);
  lockent& ent = locks[lid];
  VERIFY(ent.status == ACQUIRING || ent.status == FREE || ent.status == LOCKED);
  if (ent.status == ACQUIRING) {
	ent.revoke = true;
  } else if (ent.status == FREE) {
    ent.status = NONE; 
    pthread_mutex_unlock(&lock_mu);
	if (lu != NULL) {
	  lu->dorelease(lid);
	}
	ret = cl->call(lock_protocol::release, lid, id, r);
	VERIFY(ret == lock_protocol::OK);
    pthread_mutex_lock(&lock_mu);
	pthread_cond_signal(&ent.cond);	
  } else {
	ent.status = RELEASING;
  }
  pthread_mutex_unlock(&lock_mu);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&lock_mu);
  printf("retry_handler(%llu)\n", lid);
  lockent& ent = locks[lid];
  VERIFY(ent.status == ACQUIRING);
  ent.get_lock = true;
  pthread_cond_signal(&ent.cond);
  pthread_mutex_unlock(&lock_mu);
  return ret;
}
