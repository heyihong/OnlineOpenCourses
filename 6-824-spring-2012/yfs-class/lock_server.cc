// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
	pthread_mutex_init(&mu, NULL);
   	pthread_cond_init(&cond, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
	ScopedLock ml(&mu);
	bool& is_locked = locks[lid];
	while (is_locked) {
		pthread_cond_wait(&cond, &mu);	
	}
	is_locked = true;
	lock_protocol::status ret = lock_protocol::OK;
	return ret;	
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
	ScopedLock ml(&mu);
	locks[lid] = false;
	pthread_cond_broadcast(&cond);	
	lock_protocol::status ret = lock_protocol::OK;
	return ret;	
}
