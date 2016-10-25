// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int&)
{
  printf("acquire(%llu, %s)\n", lid, id.c_str());
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&mu);   
  std::list<std::string>& ac = lock_acquirers[lid];
  ac.push_back(id);
  if (ac.size() > 1) {
	  if (ac.size() == 2) {
	    printf("lock %llu is hold by %s, revoking\n", lid, ac.front().c_str());
	    handle h(ac.front());
	    pthread_mutex_unlock(&mu);
	    rpcc *cl = h.safebind();
	    VERIFY(cl != NULL);
		int r;
	    int rret = cl->call(rlock_protocol::revoke, lid, r);
	    VERIFY(rret == rlock_protocol::OK);
  	    pthread_mutex_lock(&mu);   
	  }
	  printf("lock %llu is still hold, %s retries\n", lid, id.c_str());
	  ret = lock_protocol::RETRY;
  } else {
	printf("lock %llu is free, %s become owner\n", lid, id.c_str());
  }
  pthread_mutex_unlock(&mu);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  printf("release(%llu, %s)\n", lid, id.c_str());
  pthread_mutex_lock(&mu);
  std::list<std::string>& acquirers = lock_acquirers[lid];
  acquirers.pop_front();
  if (!acquirers.empty()) {
	std::string acquirer_id = acquirers.front();
	bool need_revoke = acquirers.size() > 1;
  	pthread_mutex_unlock(&mu);
	handle h(acquirer_id);
	rpcc *cl = h.safebind();
	VERIFY(cl != NULL);
    int t;
	int rret = cl->call(rlock_protocol::retry, lid, t);
	VERIFY(rret == rlock_protocol::OK);
	if (need_revoke) {
		rret = cl->call(rlock_protocol::revoke, lid, t);
	    VERIFY(rret == rlock_protocol::OK);
	}
  } else {
	  lock_acquirers.erase(lid);
  	  pthread_mutex_unlock(&mu);
  }
  return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

