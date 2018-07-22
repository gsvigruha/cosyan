package com.cosyan.db.transaction;

import com.cosyan.db.conf.Config;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.lang.expr.SyntaxTree.AlterStatement;
import com.cosyan.db.lang.expr.SyntaxTree.GlobalStatement;
import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;

public class TransactionHandler {

  private long trxCntr = 0L;

  public synchronized DataTransaction begin(Iterable<Statement> statements, Config config) throws ConfigException {
    return new DataTransaction(trxCntr++, statements, config);
  }

  public synchronized Transaction begin(MetaStatement metaStatement, Config config) throws ConfigException {
    if (metaStatement instanceof AlterStatement) {
      return new AlterTransaction(trxCntr++, (AlterStatement) metaStatement, config);
    } else if (metaStatement instanceof GlobalStatement) {
      return new GlobalTransaction(trxCntr++, (GlobalStatement) metaStatement, config);
    } else {
      throw new IllegalArgumentException();
    }
  }

  public static void end() {

  }
}
