package com.cosyan.db.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.MetaResources.Resource;

public class LockManager {

  private final ReentrantReadWriteLock metaRepoLock = new ReentrantReadWriteLock();
  private final Map<String, ReentrantReadWriteLock> lockMap = new HashMap<>();

  public synchronized void metaRepoReadLock() {
    metaRepoLock.readLock().lock();
  }

  public synchronized void metaRepoWriteLock() {
    metaRepoLock.writeLock().lock();
  }

  public synchronized void metaRepoReadUnlock() {
    metaRepoLock.readLock().unlock();
  }

  public synchronized void metaRepoWriteUnlock() {
    metaRepoLock.writeLock().unlock();
  }

  public synchronized boolean tryLock(MetaResources metaResources) {
    List<Lock> locks = new ArrayList<>();
    for (Resource resource : metaResources.all()) {
      ReentrantReadWriteLock rwlock = lockMap.get(resource.getResourceId());
      assert rwlock != null : String.format("No lock '%s'", resource.getResourceId());
      Lock lock;
      if (resource.isWrite()) {
        lock = rwlock.writeLock();
      } else {
        lock = rwlock.readLock();
      }
      boolean gotLock = lock.tryLock();
      if (gotLock) {
        locks.add(lock);
      } else {
        for (Lock acquiredLock : locks) {
          acquiredLock.unlock();
        }
        return false;
      }
    }
    return true;
  }

  public synchronized void unlock(MetaResources metaResources) {
    for (Resource resource : metaResources.all()) {
      ReentrantReadWriteLock lock = lockMap.get(resource.getResourceId());
      if (resource.isWrite()) {
        lock.writeLock().unlock();
      } else {
        lock.readLock().unlock();
      }
    }
  }

  public synchronized void registerLock(String resourceId) {
    lockMap.put(resourceId, new ReentrantReadWriteLock());
  }

  public synchronized void removeLock(String resourceId) {
    lockMap.remove(resourceId);
  }
}
