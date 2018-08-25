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
package com.cosyan.db;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.auth.Authenticator;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.entity.EntityHandler;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lock.LockManager;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.session.AdminSession;
import com.cosyan.db.session.Session;
import com.cosyan.db.tools.BackupManager;
import com.cosyan.db.transaction.TransactionHandler;

public class DBApi {

  private final Config config;
  private final MetaRepo metaRepo;
  private final TransactionHandler transactionHandler;
  private final TransactionJournal transactionJournal;
  private final Authenticator authenticator;
  private final BackupManager backupManager;
  private final EntityHandler entityHandler;

  private final ThreadPoolExecutor threadPoolExecutor;
  private final ArrayBlockingQueue<Runnable> queue;

  public DBApi(Config config) throws IOException, DBException, ConfigException {
    // System.out.println("Server starting in root directory " + config.confDir());
    this.config = config;
    LockManager lockManager = new LockManager();
    authenticator = new Authenticator(config);
    metaRepo = new MetaRepo(
        config, lockManager, authenticator.grants(), new Lexer(), new Parser());
    transactionHandler = new TransactionHandler();
    transactionJournal = new TransactionJournal(config);
    backupManager = new BackupManager(config, metaRepo);
    entityHandler = new EntityHandler(metaRepo.config(), transactionHandler);
    metaRepo.init();
    // System.out.println("Server started.");
    int numThreads = config.getInt(Config.DB_NUM_THREADS);
    // TODO: figure out capacity.
    this.queue = new ArrayBlockingQueue<>(numThreads * 16);
    this.threadPoolExecutor = new ThreadPoolExecutor(numThreads, numThreads, Long.MAX_VALUE,
        TimeUnit.SECONDS, queue);
  }

  public MetaRepo getMetaRepo() {
    return metaRepo;
  }

  public EntityHandler entityHandler() {
    return entityHandler;
  }

  public Authenticator authenticator() {
    return authenticator;
  }

  public Config config() {
    return config;
  }

  public Session newAdminSession() {
    return newAdminSession(authenticator.token());
  }

  public Session newAdminSession(String token) {
    return new AdminSession(
        metaRepo,
        transactionHandler,
        transactionJournal,
        backupManager,
        AuthToken.adminToken(token),
        new Parser(),
        new Lexer());
  }

  public Session authSession(String username, String password, Authenticator.Method method) throws AuthException {
    return authSession(authenticator.auth(username, password, method));
  }

  public Session authSession(AuthToken authToken) {
    return new Session(
        metaRepo,
        transactionHandler,
        transactionJournal,
        authToken,
        new Parser(),
        new Lexer());
  }

  public void shutdown() throws IOException {
    metaRepo.shutdown();
  }

  public void execute(Runnable runnable) {
    threadPoolExecutor.execute(runnable);
  }

  public static abstract class Task implements Runnable {

    private final Session session;

    public Task(Session session) {
      this.session = session;
    }

    @Override
    public void run() {
      run(session);
    }

    public abstract void run(Session session);
    
  }
}
