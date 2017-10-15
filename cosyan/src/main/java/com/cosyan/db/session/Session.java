package com.cosyan.db.session;

import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lang.sql.Result;
import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.lang.sql.Result.ErrorResult;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.transaction.MetaTransaction;
import com.cosyan.db.transaction.Transaction;
import com.cosyan.db.transaction.TransactionHandler;
import com.google.common.collect.PeekingIterator;

public class Session {

  private final Parser parser;
  private final Lexer lexer;

  private final MetaRepo metaRepo;
  private final TransactionHandler transactionHandler;
  private final TransactionJournal transactionJournal;

  public Session(
      MetaRepo metaRepo,
      TransactionHandler transactionHandler,
      TransactionJournal transactionJournal) {
    this.metaRepo = metaRepo;
    this.transactionHandler = transactionHandler;
    this.transactionJournal = transactionJournal;
    this.parser = new Parser();
    this.lexer = new Lexer();
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
