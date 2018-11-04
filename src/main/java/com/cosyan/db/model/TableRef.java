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

import java.io.IOException;

import com.cosyan.db.lang.expr.TableDefinition.ViewDefinition;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.IParser.ParserException;

import lombok.Data;

@Data
public class TableRef {

  private final String name;
  private final String expr;
  private final int index;
  protected final transient TableMeta tableMeta;

  @Override
  public String toString() {
    return name + " [" + expr + "]";
  }

  public void reCompile(MaterializedTable tableMeta) throws ModelException, IOException {
    Parser parser = new Parser();
    Lexer lexer = new Lexer();
    try {
      Select select = parser.parseSelect(lexer.tokenizeExpression(expr + ";"));
      tableMeta.createView(new ViewDefinition(new Ident(name), select), tableMeta.owner());
    } catch (ParserException e) {
      throw new RuntimeException(e); // This should not happen.
    }
  }
}
