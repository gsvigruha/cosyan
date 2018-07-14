package com.cosyan.db.transaction;

import com.cosyan.db.lang.expr.SyntaxTree.AlterStatement;
import com.cosyan.db.lang.expr.SyntaxTree.GlobalStatement;
import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;

public class TransactionHandler {

  private long trxCntr = 0L;

  public synchronized DataTransaction begin(Iterable<Statement> statements) {
    return new DataTransaction(trxCntr++, statements);
  }

  public synchronized Transaction begin(MetaStatement metaStatement) {
    if (metaStatement instanceof AlterStatement) {
      return new AlterTransaction(trxCntr++, (AlterStatement) metaStatement);
    } else if (metaStatement instanceof GlobalStatement) {
      return new GlobalTransaction(trxCntr++, (GlobalStatement) metaStatement);
    } else {
      throw new IllegalArgumentException();
    }
  }

  public static void end() {

  }
}
