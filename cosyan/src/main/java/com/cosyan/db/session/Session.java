package com.cosyan.db.session;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.CrashResult;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.logging.MetaJournal;
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
  private final MetaJournal metaJournal;
  private final AuthToken authToken;

  public Session(
      MetaRepo metaRepo,
      TransactionHandler transactionHandler,
      TransactionJournal transactionJournal,
      MetaJournal metaJournal,
      AuthToken authToken,
      IParser parser,
      ILexer lexer) {
    this.metaRepo = metaRepo;
    this.transactionHandler = transactionHandler;
    this.transactionJournal = transactionJournal;
    this.metaJournal = metaJournal;
    this.authToken = authToken;
    this.parser = parser;
    this.lexer = lexer;
  }

  public Result execute(String sql) {
    try {
      PeekingIterator<Token> tokens = lexer.tokenize(sql);
      if (parser.isMeta(tokens)) {
        MetaStatement stmt = parser.parseMetaStatement(tokens);
        Transaction transaction = transactionHandler.begin(stmt);
        Result result = transaction.execute(metaRepo, this);
        try {
          if (result.isSuccess() && stmt.log()) {
            metaJournal.log(sql);
          }
        } catch (IOException e) {
          return new CrashResult(e);
        }
        return result;
      } else {
        Transaction transaction = transactionHandler.begin(parser.parseStatements(tokens));
        return transaction.execute(metaRepo, this);
      }
    } catch (ParserException e) {
      return new ErrorResult(e);
    }
  }

  public MetaJournal metaJournal() {
    return metaJournal;
  }

  public AuthToken authToken() {
    return authToken;
  }

  public TransactionJournal transactionJournal() {
    return transactionJournal;
  }
}
