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
package com.cosyan.db.session;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.tools.BackupManager;
import com.cosyan.db.transaction.TransactionHandler;

public class AdminSession extends Session {

  private final BackupManager backupManager;

  public AdminSession(
      MetaRepo metaRepo,
      TransactionHandler transactionHandler,
      TransactionJournal transactionJournal,
      BackupManager backupManager,
      AuthToken authToken,
      IParser parser,
      ILexer lexer) {
    super(metaRepo, transactionHandler, transactionJournal, authToken, parser, lexer);
    assert authToken.isAdmin();
    this.backupManager = backupManager;
  }

  public void backup(String name) throws DBException {
    try {
      backupManager.backup(name);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  public void restore(String name) throws DBException {
    try {
      backupManager.restore(name);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }
}
