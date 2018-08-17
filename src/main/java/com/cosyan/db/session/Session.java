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

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.lang.expr.Statements.MetaStatement;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.session.IParser.ParserException;
import com.cosyan.db.transaction.Transaction;
import com.cosyan.db.transaction.TransactionHandler;
import com.google.common.collect.PeekingIterator;

public class Session {

  private final IParser parser;
  private final ILexer lexer;

  private final MetaRepo metaRepo;
  private final TransactionHandler transactionHandler;
  private final TransactionJournal transactionJournal;
  private final AuthToken authToken;

  private Transaction lastTransaction = null;

  public Session(MetaRepo metaRepo, TransactionHandler transactionHandler, TransactionJournal transactionJournal, AuthToken authToken,
      IParser parser, ILexer lexer) {
    this.metaRepo = metaRepo;
    this.transactionHandler = transactionHandler;
    this.transactionJournal = transactionJournal;
    this.authToken = authToken;
    this.parser = parser;
    this.lexer = lexer;
  }

  public Result execute(String sql) {
    try {
      Transaction transaction = transaction(sql);
      return execute(transaction);
    } catch (ParserException | ConfigException e) {
      return new ErrorResult(e);
    }
  }

  private Transaction transaction(String sql) throws ConfigException, ParserException {
    PeekingIterator<Token> tokens = lexer.tokenize(sql);
    if (parser.isMeta(tokens)) {
      MetaStatement stmt = parser.parseMetaStatement(tokens);
      return transactionHandler.begin(stmt, metaRepo.config());
    } else {
      return transactionHandler.begin(parser.parseStatements(tokens), metaRepo.config());
    }
  }

  public Result execute(Transaction transaction) {
    synchronized (this) {
      if (lastTransaction != null) {
        return new ErrorResult(new IllegalStateException("Already executing."));
      }
      lastTransaction = transaction;
    }
    try {
      return lastTransaction.execute(metaRepo, this);
    } finally {
      synchronized (this) {
        lastTransaction = null;
      }
    }
  }

  public synchronized void cancel() {
    if (lastTransaction != null) {
      lastTransaction.cancel();
    }
  }

  public AuthToken authToken() {
    return authToken;
  }

  public TransactionJournal transactionJournal() {
    return transactionJournal;
  }

  public MetaRepo metaRepo() throws AuthException {
    if (authToken.isAdmin()) {
      return metaRepo;
    } else {
      throw new AuthException("Only admins can access metaRepo.");
    }
  }
}
