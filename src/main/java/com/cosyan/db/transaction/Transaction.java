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
package com.cosyan.db.transaction;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.session.Session;

public abstract class Transaction {

  protected final long trxNumber;
  private final int retryMS;

  protected AtomicBoolean cancelled = new AtomicBoolean(false);

  public Transaction(long trxNumber, int retryMS) {
    this.trxNumber = trxNumber;
    this.retryMS = retryMS;
  }

  public long getTrxNumber() {
    return trxNumber;
  }

  protected void lock(MetaResources metaResources, MetaRepo metaRepo) {
    boolean locked = false;
    Random random = new Random();
    while (!locked && !cancelled.get()) {
      if (metaRepo.tryLock(metaResources)) {
        locked = true;
      } else {
        try {
          Thread.sleep(random.nextInt(retryMS));
        } catch (InterruptedException e) {
          cancelled.set(true);
        }
      }
    }
  }

  public abstract Result execute(MetaRepo metaRepo, Session session);

  public abstract void cancel();
}
