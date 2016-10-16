package simpledb;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

/**
 * Created by heyihong on 2016/10/15.
 */
public class LockManager {

    private static class RwLock {

        private Integer numRead;

        private Semaphore semaphore;

        public RwLock() {
            this.numRead = 0;
            this.semaphore = new Semaphore(1);
        }

        public void readLock() {
            synchronized (this.numRead) {
                if (this.numRead == 0) {
                    try {
                        this.semaphore.acquire();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                ++this.numRead;
            }
        }

        public void readUnlock() {
            synchronized (this.numRead) {
                --this.numRead;
                if (this.numRead == 0) {
                    this.semaphore.release();
                }
            }
        }

        public void writeLock() {
            try {
                this.semaphore.acquire();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        public void writeUnlock() {
            this.semaphore.release();
        }
    }

    private static class LockInfo {

        public boolean exclusive;

        public Set<TransactionId> owners;

        public Set<TransactionId> acquirers;

        public LockInfo() {
            this.exclusive = false;
            this.owners = new HashSet<TransactionId>();
            this.acquirers = new HashSet<TransactionId>();
        }
    }

    private Map<TransactionId, Set<TransactionId>> depGraph;

    private Map<PageId, LockInfo> pidToLockInfo;

    private Map<PageId, RwLock> pidToRwLock;

    private Map<TransactionId, Set<PageId>> tidToPids;

    public LockManager() {
        this.depGraph = new HashMap<TransactionId, Set<TransactionId>>();
        this.pidToLockInfo = new HashMap<PageId, LockInfo>();
        this.pidToRwLock = new HashMap<PageId, RwLock>();
        this.tidToPids = new HashMap<TransactionId, Set<PageId>>();
    }

    public void acquireSharedLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        System.out.println(Thread.currentThread().getId() + " Start acquiring shared lock: " + tid + " " + pid);
        RwLock lock;
        LockInfo lockInfo;
        synchronized (this) {
            lockInfo = this.getOrCreateLockInfo(pid);
            if (lockInfo.owners.contains(tid)) {
                return;
            }
            if (!lockInfo.owners.isEmpty() && lockInfo.exclusive) {
                this.depGraph.put(tid, lockInfo.owners);
                if (this.hasDeadlock(tid)) {
                    this.depGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
            lockInfo.acquirers.add(tid);
            lock = this.getOrCreateRwLock(pid);
        }
        lock.readLock();
        synchronized (this) {
            this.depGraph.remove(tid);
            lockInfo.exclusive = false;
            lockInfo.acquirers.remove(tid);
            lockInfo.owners.add(tid);
            this.getOrCreatePids(tid).add(pid);
        }
        System.out.println(Thread.currentThread().getId() + " End acquiring shared lock: " + tid + " " + pid);
    }

    public void acquireExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        System.out.println(Thread.currentThread().getId() + " Start acquiring exclusive lock: " + tid + " " + pid);
        RwLock lock;
        LockInfo lockInfo;
        synchronized (this) {
            lockInfo = this.getOrCreateLockInfo(pid);
            if (lockInfo.owners.contains(tid)) {
                if (lockInfo.exclusive) {
                    return;
                }
                if (lockInfo.owners.size() > 1 || !lockInfo.acquirers.isEmpty()) {
                    throw new TransactionAbortedException();
                }
                releaseLock(tid, pid);
            }
            if (!lockInfo.owners.isEmpty()) {
                this.depGraph.put(tid, lockInfo.owners);
                if (this.hasDeadlock(tid)) {
                    this.depGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
            lockInfo.acquirers.add(tid);
            lock = this.getOrCreateRwLock(pid);
        }
        lock.writeLock();
        synchronized (this) {
            this.depGraph.remove(tid);
            lockInfo.exclusive = true;
            lockInfo.acquirers.remove(tid);
            lockInfo.owners.add(tid);
            this.getOrCreatePids(tid).add(pid);
        }
        System.out.println(Thread.currentThread().getId() + " End acquiring exclusive lock: " + tid + " " + pid);
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        Set<PageId> pids = this.tidToPids.get(tid);
        return pids != null && pids.contains(pid);
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        Set<PageId> pids = this.tidToPids.get(tid);
        if (pids != null && pids.remove(pid)) {
            LockInfo lockInfo = this.pidToLockInfo.get(pid);
            RwLock rwLock = this.pidToRwLock.get(pid);
            lockInfo.owners.remove(tid);
            if (lockInfo.exclusive) {
                rwLock.writeUnlock();
            } else {
                rwLock.readUnlock();
            }
        }
    }

    public synchronized void releaseLocks(TransactionId tid) {
        Set<PageId> pids = this.tidToPids.remove(tid);
        if (pids != null) {
            for (PageId pid : pids) {
                System.out.println(Thread.currentThread().getId() + " Release lock " + tid + " " + pid);
                LockInfo lockInfo = this.pidToLockInfo.get(pid);
                RwLock rwLock = this.pidToRwLock.get(pid);
                lockInfo.owners.remove(tid);
                if (lockInfo.exclusive) {
                    rwLock.writeUnlock();
                } else {
                    rwLock.readUnlock();
                }
            }
        }
    }

    private Set<PageId> getOrCreatePids(TransactionId tid) {
        Set<PageId> pids = this.tidToPids.get(tid);
        if (pids == null) {
            pids = new HashSet<PageId>();
            this.tidToPids.put(tid, pids);
        }
        return pids;
    }

    private LockInfo getOrCreateLockInfo(PageId pid) {
        LockInfo lockInfo = this.pidToLockInfo.get(pid);
        if (lockInfo == null) {
            lockInfo = new LockInfo();
            this.pidToLockInfo.put(pid, lockInfo);
        }
        return lockInfo;
    }

    private RwLock getOrCreateRwLock(PageId pid) {
        RwLock rwlock = this.pidToRwLock.get(pid);
        if (rwlock == null) {
            rwlock = new RwLock();
            this.pidToRwLock.put(pid, rwlock);
        }
        return rwlock;
    }


        /**
         * detect whether tid is in a deadlock
         * @param tid
         * @return whether has deadlock
         */
    private boolean hasDeadlock(TransactionId tid) {
        Deque<TransactionId> qu = new LinkedList<TransactionId>();
        Set<TransactionId> visited = new HashSet<TransactionId>();
        visited.add(tid);
        qu.push(tid);
        while (!qu.isEmpty()) {
            TransactionId x = qu.pop();
            Set<TransactionId> adjs = this.depGraph.get(x);
            if (adjs != null) {
                for (TransactionId y : adjs) {
                    if (!visited.contains(y)) {
                        visited.add(y);
                        qu.push(y);
                    } else if (y.equals(tid)) {
                        System.out.println("Transaction " + tid + " will deadlock");
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
