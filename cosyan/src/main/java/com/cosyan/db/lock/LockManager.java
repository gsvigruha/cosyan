package com.cosyan.db.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableList;

public class LockManager {

  private final Map<String, ReentrantReadWriteLock> lockMap = new HashMap<>();

  public synchronized boolean tryLock(ImmutableList<ResourceLock> locks) {
    for (int i = 0; i < locks.size(); i++) {
      ReentrantReadWriteLock lock = lockMap.get(locks.get(i).getResourceId());
      boolean gotLock;
      if (locks.get(i).isWrite()) {
        gotLock = lock.writeLock().tryLock();
      } else {
        gotLock = lock.readLock().tryLock();
      }
      if (!gotLock) {
        for (int j = 0; j < i; j++) {
          ReentrantReadWriteLock acquiredLock = lockMap.get(locks.get(j).getResourceId());
          if (locks.get(j).isWrite()) {
            acquiredLock.writeLock().unlock();
          } else {
            acquiredLock.readLock().unlock();
          }
        }
        return false;
      }
    }
    return true;
  }

  public synchronized void unlock(ImmutableList<ResourceLock> locks) {
    for (ResourceLock lock : locks) {
      ReentrantReadWriteLock l = lockMap.get(lock.getResourceId());
      if (lock.isWrite()) {
        l.writeLock().unlock();
      } else {
        l.readLock().unlock();
      }
    }
  }

  public synchronized void registerLock(String resourceId) {
    lockMap.put(resourceId, new ReentrantReadWriteLock());
  }
}
