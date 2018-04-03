package com.cosyan.db.transaction;

import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;

public class TransactionHandler {

  private long trxCntr = 0L;

  public synchronized Transaction begin(Iterable<Statement> statements) {
    return new Transaction(trxCntr++, statements);
  }

  public synchronized MetaTransaction begin(MetaStatement metaStatement) {
    return new MetaTransaction(trxCntr++, metaStatement);
  }

  public static void end() {

  }
}
