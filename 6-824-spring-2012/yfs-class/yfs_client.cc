// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client_cache.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::lock_release_extent::lock_release_extent(extent_client* _client) 
{
  client = _client;  
}

void yfs_client::lock_release_extent::dorelease(lock_protocol::lockid_t lid) 
{
  client->flush(lid);
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lu = new lock_release_extent(ec);
  lc = new lock_client_cache(lock_dst, lu);
}

yfs_client::~yfs_client() {
  delete ec;
  delete lu;
  delete lc;
}

std::vector<yfs_client::dirent>
yfs_client::string2dirents(const std::string& buf) 
{
	std::istringstream ist(buf.c_str());
	yfs_client::dirent dr;
	std::string inum_str;
	std::vector<yfs_client::dirent> dirents;
	while (std::getline(ist, dr.name)) {
		std::getline(ist, inum_str);	
		std::istringstream inum_st(inum_str.c_str());
		inum_st >> dr.inum;
		dirents.push_back(dr);
	}
	return dirents;
}

std::string 
yfs_client::dirents2string(const std::vector<yfs_client::dirent>& dirents)
{
	std::ostringstream ost;
	std::vector<yfs_client::dirent>::const_iterator iter;
	for (iter = dirents.begin(); iter != dirents.end(); iter++) {
		ost << iter->name << "\n" << iter->inum << "\n";
	}
	return ost.str();
}

std::vector<yfs_client::dirent>::iterator
yfs_client::finddirent(std::vector<yfs_client::dirent>& dirents, const std::string& name) 
{
	std::vector<yfs_client::dirent>::iterator iter;
	for (iter = dirents.begin(); iter != dirents.end(); iter++) {
		if (iter->name == name) {
			break;
		}
	}
	return iter;
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  lc->acquire(inum);
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the file lock

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:
  lc->release(inum);
  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  lc->acquire(inum);
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the directory lock

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  lc->release(inum);
  return r;
}

int
yfs_client::create(inum parent, const dirent& dr)
{
  lc->acquire(parent);
  int r = OK;
  std::string buf;
  std::vector<dirent> dirents;
  std::vector<dirent>::iterator iter;
  printf("create %s\n", dr.name.c_str());
  if (ec->get(parent, buf) != extent_protocol::OK) {
	  r = IOERR;
	  goto releasedl;
  }
  dirents = string2dirents(buf);
  iter = finddirent(dirents, dr.name);
  if (iter != dirents.end()) {
	  r = EXIST;
	  goto releasedl;
  }
  lc->acquire(dr.inum);
  if (ec->put(dr.inum, "") != extent_protocol::OK) {
    r = IOERR;
    goto releasefl;
  }
  buf += dirents2string(std::vector<dirent>(1, dr));
  if (ec->put(parent, buf) != extent_protocol::OK) {
	  r = IOERR;
	  goto releasefl;
  }
 releasefl:
  lc->release(dr.inum);
 releasedl:
  lc->release(parent);
  return r;
}

int
yfs_client::getdirents(inum inum, std::vector<dirent>& dirents)
{
  lc->acquire(inum);
  int r = OK;

  printf("getdirents %016llx\n", inum);
  std::string buf;
  if (ec->get(inum, buf) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  dirents = string2dirents(buf);
 release:
  lc->release(inum);
  return r;
}

int
yfs_client::getbuf(inum inum, size_t off, size_t size, std::string& buf) 
{
	lc->acquire(inum);
	int r = OK;
	printf("getbuf %d %d %d\n", inum, off, size);
	if (ec->get(inum, buf) != extent_protocol::OK) {
		r = IOERR;
		goto release;
	}
	buf = buf.substr(off, size);	
	printf("result = %s\n", buf.c_str());
   release:
	lc->release(inum);
	return r;
}

int
yfs_client::setbuf(inum inum, size_t off, const std::string& buf, bool is_end)
{
	lc->acquire(inum);
	int r = OK;
	std::string str;
	printf("setbuf %d, %d, %s\n", inum, off, buf.c_str());
	if (ec->get(inum, str) != extent_protocol::OK) {
		r = IOERR;
		goto release;
	}
	if (is_end || off + buf.size() > str.size()) {
		str.resize(off + buf.size());
	}
	str.replace(off, buf.size(), buf);
	if (ec->put(inum, str) != extent_protocol::OK) {
		r = IOERR;
		goto release;
	}
   release:
	lc->release(inum);
	return r;
}

int
yfs_client::rmfile(inum parent, const std::string& name) 
{
	lc->acquire(parent);
	int r = OK;
	std::string buf;
	inum inum;
	std::vector<dirent> dirents;
	std::vector<dirent>::iterator iter;
	printf("rmfile %s\n", name.c_str());
	if (ec->get(parent, buf) != extent_protocol::OK) {
		r = IOERR;
		goto releasedl;
	}
	dirents = string2dirents(buf);
	iter = finddirent(dirents, name);
	if (iter == dirents.end()) {
		r = NOENT;
		goto releasefl;
	}
	inum = iter->inum;
	lc->acquire(inum);
	if (ec->remove(inum) != extent_protocol::OK) {
		r = IOERR;
		goto releasefl;
	}
	dirents.erase(iter);
	buf = dirents2string(dirents);
	if (ec->put(parent, buf) != extent_protocol::OK) {
		r = IOERR;
		goto releasefl;
	}
   releasefl:
	lc->release(inum);
   releasedl:
	lc->release(parent);
	return r;
}

