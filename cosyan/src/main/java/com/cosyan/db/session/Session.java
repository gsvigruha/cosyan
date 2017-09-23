package com.cosyan.db.session;

import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.sql.Parser;
import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.Result;
import com.cosyan.db.sql.Result.ErrorResult;
import com.cosyan.db.sql.SyntaxTree;
import com.cosyan.db.transaction.Transaction;
import com.cosyan.db.transaction.TransactionHandler;

import lombok.Data;

@Data
public class Session {

  private final Parser parser;

  private final MetaRepo metaRepo;
  private final TransactionHandler transactionHandler;
  private final TransactionJournal transactionJournal;

  public Result execute(String sql) {
    try {
      SyntaxTree tree = parser.parse(sql);
      Transaction transaction = transactionHandler.begin(tree.getRoots());
      return transaction.execute(metaRepo, transactionJournal);
    } catch (ParserException e) {
      return new ErrorResult(e);
    }
  }
}
