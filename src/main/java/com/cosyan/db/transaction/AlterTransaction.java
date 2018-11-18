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

import java.io.IOException;

import com.cosyan.db.conf.Config;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.lang.expr.Statements.AlterStatement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.CrashResult;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.MetaWriter;
import com.cosyan.db.session.Session;
import com.cosyan.db.transaction.MetaResources.MetaResource;

public class AlterTransaction extends Transaction {

  private final AlterStatement alterStatement;

  public AlterTransaction(long trxNumber, AlterStatement alterStatement, Config config) throws ConfigException {
    super(trxNumber, config.getInt(Config.TR_RETRY_MS));
    this.alterStatement = alterStatement;
  }

  @Override
  public Result execute(MetaRepo metaRepo, Session session) {
    TransactionJournal journal = session.transactionJournal();
    MetaWriter metaWriter = metaRepo.metaRepoWriteLock();
    MetaResources metaResources;
    try {
      try {
        metaResources = alterStatement.executeMeta(metaWriter, session.authToken());
        for (MetaResource resource : metaResources.objects()) {
          metaWriter.checkAccess(resource, session.authToken());
        }
      } catch (ModelException | GrantException | IOException e) {
        return new ErrorResult(e);
      }
      try {
        lock(metaResources, metaRepo);
        journal.start(trxNumber);
        Result result;
        Resources resources = metaRepo.resources(metaResources, session.authToken());
        try {
          result = alterStatement.executeData(metaWriter, resources);
        } catch (RuleException e) {
          resources.rollback();
          metaRepo.readTables();
          journal.userError(trxNumber);
          return new ErrorResult(e);
        } catch (IOException e) {
          resources.rollback();
          metaRepo.readTables();
          journal.ioReadError(trxNumber);
          return new CrashResult(e);
        }
        try {
          resources.commit();
          metaRepo.writeTables();
          journal.success(trxNumber);
          return result;
        } catch (IOException e) {
          // Need to restore db;
          journal.ioWriteError(trxNumber);
          e.printStackTrace();
          return new CrashResult(e);
        }
      } catch (Throwable e) {
        // Unspecified error, need to restore db;
        try {
          journal.crash(trxNumber);
        } catch (DBException e1) {
          return new CrashResult(e);
        }
        return new CrashResult(e);
      } finally {
        metaRepo.unlock(metaResources);
      }
    } finally {
      metaWriter.metaRepoWriteUnlock();
    }
  }

  @Override
  public void cancel() {
    cancelled.set(true);
  }
}
