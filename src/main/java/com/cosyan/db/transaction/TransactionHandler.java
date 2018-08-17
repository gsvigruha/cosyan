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
package com.cosyan.db.transaction;

import com.cosyan.db.conf.Config;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.lang.expr.Statements.AlterStatement;
import com.cosyan.db.lang.expr.Statements.GlobalStatement;
import com.cosyan.db.lang.expr.Statements.MetaStatement;
import com.cosyan.db.lang.expr.Statements.Statement;

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
