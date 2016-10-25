// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

std::map<extent_protocol::extentid_t, extent_client::extent_ent> extent_client::extents;
pthread_mutex_t extent_client::mu = PTHREAD_MUTEX_INITIALIZER;

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_client::~extent_client() 
{
  cl->cancel();
  delete cl; 
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{   
  printf("get(%llu)\n", eid);
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mu);
  extent_ent& ent = extents[eid];
  pthread_mutex_unlock(&mu);
  if (!(ent.bits & BUF_BIT)) {
    ret = cl->call(extent_protocol::get, eid, ent.buf);
    VERIFY(ret == extent_protocol::OK);
	ent.bits |= BUF_BIT;
  }
  buf = ent.buf;
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  printf("getattr(%llu)\n", eid);
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mu);
  extent_ent& ent = extents[eid];
  pthread_mutex_unlock(&mu);
  if (!(ent.bits & ATTR_BIT)) {
    ret = cl->call(extent_protocol::getattr, eid, ent.attr);
	VERIFY(ret == extent_protocol::OK);
	ent.bits |= ATTR_BIT;
  }
  attr = ent.attr;
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  printf("put(%llu, %s)\n", eid, buf.c_str());
  extent_protocol::status ret;
  pthread_mutex_lock(&mu);
  extent_ent& ent = extents[eid];
  pthread_mutex_unlock(&mu);
  if (!(ent.bits & ATTR_BIT)) {
    ret = cl->call(extent_protocol::getattr, eid, ent.attr);
	VERIFY(ret == extent_protocol::OK);
	ent.bits |= ATTR_BIT;
  }
  ent.attr.mtime = ent.attr.ctime = time(NULL);
  ent.attr.size = buf.size();
  ent.buf = buf;	
  ent.bits |= BUF_BIT | DIRTY_BIT;
  ent.bits &= ~DELETED_BIT;
  return extent_protocol::OK;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  printf("remove(%llu)\n", eid);
  pthread_mutex_lock(&mu);
  extent_ent& ent = extents[eid];
  pthread_mutex_unlock(&mu);
  ent.buf = "";
  ent.attr = extent_protocol::attr();
  ent.bits |= DIRTY_BIT | DELETED_BIT;
  return extent_protocol::OK;
}

void
extent_client::flush(extent_protocol::extentid_t eid)
{
  printf("flush(%llu)\n", eid);
  pthread_mutex_lock(&mu);
  int r;
  int ret;
  extent_ent& ent = extents[eid];
  pthread_mutex_unlock(&mu);
  if (ent.bits & DELETED_BIT) { 
	ret = cl->call(extent_protocol::remove, eid, r);
	VERIFY(ret == extent_protocol::OK);
  } else if (ent.bits & DIRTY_BIT) { 
	ret = cl->call(extent_protocol::put, eid, ent.buf, r);
	VERIFY(ret == extent_protocol::OK);
  }
  pthread_mutex_lock(&mu);
  extents.erase(eid);
  pthread_mutex_unlock(&mu);
}
