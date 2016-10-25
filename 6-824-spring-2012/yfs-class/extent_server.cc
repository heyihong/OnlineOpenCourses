// the extent server implementation

#include "extent_server.h"
#include "slock.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
  pthread_mutex_init(&mu, NULL);
}

extent_server::~extent_server() {}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  // You fill this in for Lab 2.
  ScopedLock ml(&mu);
  printf("put %d %s\n", id, buf.c_str());
  extent_t& extent = extents[id];
  extent.buf = buf;
  extent.attr.size = buf.size();
  extent.attr.ctime = extent.attr.mtime = time(NULL);
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  // You fill this in for Lab 2.
  ScopedLock ml(&mu);
  extent_t& extent = extents[id];
  printf("get %d %s\n", id, extent.buf.c_str());
  buf = extent.buf;
  extent.attr.atime = time(NULL);
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  ScopedLock ml(&mu);
  printf("getattr %d\n", id);
  a = extents[id].attr;
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  // You fill this in for Lab 2.
  ScopedLock ml(&mu);
  printf("remove %d\n", id);
  extents.erase(id);
  return extent_protocol::OK;
}

