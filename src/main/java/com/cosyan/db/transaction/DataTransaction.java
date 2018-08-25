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
import java.util.ArrayList;
import java.util.List;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.conf.Config;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.lang.expr.Statements.Statement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.CrashResult;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.TransactionResult;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.session.Session;
import com.cosyan.db.transaction.MetaResources.TableMetaResource;
import com.google.common.collect.ImmutableList;

public class DataTransaction extends Transaction {
  private final ImmutableList<Statement> statements;

  public DataTransaction(long trxNumber, Iterable<Statement> statements, Config config) throws ConfigException {
    super(trxNumber, config.getInt(Config.TR_RETRY_MS));
    this.statements = ImmutableList.copyOf(statements);
  }

  public ImmutableList<Statement> getStatements() {
    return statements;
  }

  protected MetaResources collectResources(MetaRepo metaRepo, AuthToken authToken) throws ModelException {
    MetaResources metaResources = MetaResources.empty();
    for (Statement statement : statements) {
      metaResources = metaResources.merge(statement.compile(metaRepo, authToken));
    }
    return metaResources;
  }

  protected Result execute(MetaRepo metaRepo, Resources resources) throws RuleException, IOException {
    List<Result> results = new ArrayList<>();
    for (Statement statement : statements) {
      results.add(statement.execute(resources));
    }
    return new TransactionResult(results);
  }

  @Override
  public void cancel() {
    cancelled.set(true);
    for (Statement statement : statements) {
      statement.cancel();
    }
  }

  @Override
  public Result execute(MetaRepo metaRepo, Session session) {
    TransactionJournal journal = session.transactionJournal();
    metaRepo.metaRepoReadLock();
    MetaResources metaResources;
    try {
      metaResources = collectResources(metaRepo, session.authToken());
    } catch (ModelException e) {
      return new ErrorResult(e);
    } finally {
      metaRepo.metaRepoReadUnlock();
    }
    try {
      for (TableMetaResource resource : metaResources.tables()) {
        metaRepo.checkAccess(resource, session.authToken());
      }
    } catch (GrantException e) {
      return new ErrorResult(e);
    }
    try {
      lock(metaResources, metaRepo);
      journal.start(trxNumber);
      Result result;
      Resources resources = metaRepo.resources(metaResources, session.authToken());
      try {
        result = execute(metaRepo, resources);
      } catch (RuleException e) {
        resources.rollback();
        journal.userError(trxNumber);
        return new ErrorResult(e);
      } catch (IOException e) {
        resources.rollback();
        journal.ioReadError(trxNumber);
        return new CrashResult(e);
      }
      try {
        resources.commit();
        journal.success(trxNumber);
        return result;
      } catch (IOException e) {
        // Need to restore db;
        e.printStackTrace();
        journal.ioWriteError(trxNumber);
        return new CrashResult(e);
      }
    } catch (Throwable e) {
      // Unspecified error, need to restore db;
      e.printStackTrace();
      try {
        journal.crash(trxNumber);
      } catch (DBException e1) {
        return new CrashResult(e);
      }
      return new CrashResult(e);
    } finally {
      metaRepo.unlock(metaResources);
    }
  }
}
