/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.cosyan.db.meta.MaterializedTable;
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
    for (Resource resource : metaResources.lockResources()) {
      ReentrantReadWriteLock rwlock = lockMap.get(resource.getResourceId());
      assert rwlock != null : String.format("Invalid resource '%s'. Existing: %s.", resource.getResourceId(), lockMap.keySet());
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
    for (Resource resource : metaResources.lockResources()) {
      ReentrantReadWriteLock lock = lockMap.get(resource.getResourceId());
      assert lock != null : String.format("Invalid resource '%s'. Existing: %s.", resource.getResourceId(), lockMap.keySet());
      if (resource.isWrite()) {
        lock.writeLock().unlock();
      } else {
        lock.readLock().unlock();
      }
    }
  }

  public synchronized void registerLock(MaterializedTable table) {
    lockMap.put(table.fullName(), new ReentrantReadWriteLock());
  }

  public synchronized void removeLock(MaterializedTable table) {
    lockMap.remove(table.fullName());
  }

  public synchronized void syncLocks(List<MaterializedTable> tables) {
    Set<String> ids = tables.stream().map(t -> t.fullName()).collect(Collectors.toSet());
    for (String resourceId : ids) {
      if (!lockMap.containsKey(resourceId)) {
        lockMap.put(resourceId, new ReentrantReadWriteLock());
      }
    }
    Set<String> oldIds = new HashSet<>(lockMap.keySet());
    for (String resourceId : oldIds) {
      if (!ids.contains(resourceId)) {
        lockMap.remove(resourceId);
      }
    }
  }
}
