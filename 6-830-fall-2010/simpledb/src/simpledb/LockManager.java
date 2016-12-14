package simpledb;

import java.util.*;

/**
 * Created by heyihong on 2016/10/15.
 */
public class LockManager {

    private static class RwLock {

        private int numRead;

        private boolean isExclusive;

        public RwLock() {
            this.numRead = 0;
            this.isExclusive = false;
        }

        public synchronized void readLock() {
            try {
                while (this.isExclusive) {
                    this.wait();
                }
                ++this.numRead;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public synchronized void readUnlock() {
            --this.numRead;
            this.notify();
        }

        public synchronized void writeLock() {
            try {
                while (this.numRead > 0 || this.isExclusive) {
                    this.wait();
                }
                this.isExclusive = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        public synchronized void writeUnlock() {
            this.isExclusive = false;
            this.notify();
        }

        public synchronized void upgradeLock() {
            try {
                while (this.numRead > 1) {
                    this.wait();
                }
                this.numRead = 0;
                this.isExclusive = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class LockInfo {

        public boolean exclusive;

        public Set<TransactionId> holders;

        public LockInfo() {
            this.exclusive = false;
            this.holders = new HashSet<TransactionId>();
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
//        System.out.println(Thread.currentThread().getId() + " Start acquiring shared lock: " + tid + " " + pid);
        RwLock lock;
        LockInfo lockInfo;
        synchronized (this) {
            lockInfo = this.getOrCreateLockInfo(pid);
            if (lockInfo.holders.contains(tid)) {
                return;
            }
            if (!lockInfo.holders.isEmpty() && lockInfo.exclusive) {
                this.depGraph.put(tid, lockInfo.holders);
                if (this.hasDeadlock(tid)) {
                    this.depGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
            lock = this.getOrCreateRwLock(pid);
        }
        lock.readLock();
        synchronized (this) {
            this.depGraph.remove(tid);
            lockInfo.exclusive = false;
            lockInfo.holders.add(tid);
            this.getOrCreatePids(tid).add(pid);
        }
//        System.out.println(Thread.currentThread().getId() + " End acquiring shared lock: " + tid + " " + pid);
    }

    public void acquireExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
//        System.out.println(Thread.currentThread().getId() + " Start acquiring exclusive lock: " + tid + " " + pid);
        boolean isUpgrade = false;
        RwLock lock;
        LockInfo lockInfo;
        synchronized (this) {
            lockInfo = this.getOrCreateLockInfo(pid);
            if (lockInfo.holders.contains(tid)) {
                if (lockInfo.exclusive) {
                    return;
                }
                // need to upgrade from shared lock to exclusive lock
                isUpgrade = true;
            }
            // the dependency graph may contain self-loop for upgrade
            this.depGraph.put(tid, lockInfo.holders);
            if (this.hasDeadlock(tid)) {
                this.depGraph.remove(tid);
                throw new TransactionAbortedException();
            }
            lock = this.getOrCreateRwLock(pid);
        }
        if (isUpgrade) {
            lock.upgradeLock();
        } else {
            lock.writeLock();
        }
        synchronized (this) {
            this.depGraph.remove(tid);
            lockInfo.exclusive = true;
            lockInfo.holders.add(tid);
            this.getOrCreatePids(tid).add(pid);
        }
//        System.out.println(Thread.currentThread().getId() + " End acquiring exclusive lock: " + tid + " " + pid);
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
            lockInfo.holders.remove(tid);
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
//                System.out.println(Thread.currentThread().getId() + " Release lock " + tid + " " + pid);
                LockInfo lockInfo = this.pidToLockInfo.get(pid);
                RwLock rwLock = this.pidToRwLock.get(pid);
                lockInfo.holders.remove(tid);
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
     *
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
                    // except self-loop condition
                    if (!x.equals(tid) && y.equals(tid)) {
                        return true;
                    }
                    if (!visited.contains(y)) {
                        visited.add(y);
                        qu.push(y);
                    }
                }
            }
        }
        return false;
    }
}
