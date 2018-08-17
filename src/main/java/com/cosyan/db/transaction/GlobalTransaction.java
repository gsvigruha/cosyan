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
import com.cosyan.db.lang.expr.Statements.GlobalStatement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.CrashResult;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.Session;

public class GlobalTransaction extends Transaction {

  private final GlobalStatement globalStatement;

  public GlobalTransaction(long trxNumber, GlobalStatement globalStatement, Config config) throws ConfigException {
    super(trxNumber, config.getInt(Config.TR_RETRY_MS));
    this.globalStatement = globalStatement;
  }

  @Override
  public Result execute(MetaRepo metaRepo, Session session) {
    TransactionJournal journal = session.transactionJournal();
    metaRepo.metaRepoWriteLock();
    try {
      try {
        journal.start(trxNumber);
        Result result = globalStatement.execute(metaRepo, session.authToken());
        metaRepo.writeTables();
        return result;
      } catch (ModelException | GrantException e) {
        // Restore metaRepo.
        metaRepo.readTables();
        journal.userError(trxNumber);
        return new ErrorResult(e);
      } catch (IOException e) {
        // Restore metaRepo.
        metaRepo.readTables();
        journal.ioWriteError(trxNumber);
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
      metaRepo.metaRepoWriteUnlock();
    }
  }

  @Override
  public void cancel() {
    // Global statements cannot be cancelled.
  }
}
