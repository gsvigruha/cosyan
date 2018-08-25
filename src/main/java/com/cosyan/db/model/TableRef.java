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
package com.cosyan.db.model;

import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.TableDefinition.AggRefDefinition;
import com.cosyan.db.lang.expr.TableDefinition.FlatRefDefinition;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.IParser.ParserException;
import com.google.common.collect.ImmutableList;

import lombok.Data;

@Data
public class TableRef {

  private final String name;
  private final String expr;
  private final int index;
  private final boolean aggr;
  protected final transient TableMeta tableMeta;

  @Override
  public String toString() {
    return name + " [" + expr + "]";
  }

  public void reCompile(MaterializedTable tableMeta) throws ModelException {
    Parser parser = new Parser();
    Lexer lexer = new Lexer();
    try {
      if (aggr) {
        Select select = parser.parseSelect(lexer.tokenizeExpression(expr + ";"));
        tableMeta.createAggRef(new AggRefDefinition(new Ident(name), select), tableMeta.owner());
      } else {
        ImmutableList<Expression> exprs = parser.parseExpressions(lexer.tokenize(expr + ";"));
        tableMeta.createFlatRef(new FlatRefDefinition(new Ident(name), exprs));
      }
    } catch (ParserException e) {
      throw new RuntimeException(e); // This should not happen.
    }
  }
}
