package com.cosyan.db.transaction;

import com.cosyan.db.sql.SyntaxTree.Statement;

public class TransactionHandler {

  private long trxCntr = 0L;

  public synchronized Transaction begin(Iterable<Statement> statements) {
    return new Transaction(trxCntr++, statements);
  }

  public static void end() {

  }
}
