package com.cosyan.db.session;

import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.session.IParser.ParserException;
import com.cosyan.db.transaction.MetaTransaction;
import com.cosyan.db.transaction.Transaction;
import com.cosyan.db.transaction.TransactionHandler;
import com.google.common.collect.PeekingIterator;

public class Session {

  private final IParser parser;
  private final ILexer lexer;

  private final MetaRepo metaRepo;
  private final TransactionHandler transactionHandler;
  private final TransactionJournal transactionJournal;

  public Session(
      MetaRepo metaRepo,
      TransactionHandler transactionHandler,
      TransactionJournal transactionJournal,
      IParser parser,
      ILexer lexer) {
    this.metaRepo = metaRepo;
    this.transactionHandler = transactionHandler;
    this.transactionJournal = transactionJournal;
    this.parser = parser;
    this.lexer = lexer;
  }

  public Result execute(String sql) {
    try {
      PeekingIterator<Token> tokens = lexer.tokenize(sql);
      if (parser.isMeta(tokens)) {
        MetaTransaction transaction = transactionHandler.begin(parser.parseMetaStatement(tokens));
        return transaction.execute(metaRepo, transactionJournal);
      } else {
        Transaction transaction = transactionHandler.begin(parser.parseStatements(tokens));
        return transaction.execute(metaRepo, transactionJournal);
      }
    } catch (ParserException e) {
      return new ErrorResult(e);
    }
  }
}
