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
package com.cosyan.db.entity;

import java.io.IOException;

import com.cosyan.db.lang.expr.BinaryExpression;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.FuncCallExpression;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.lang.sql.UpdateStatement.SetExpression;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableContext;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class EntityFields {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Constant extends Expression {

    private final Ident ident;
    private final String value;

    @Override
    public ColumnMeta compile(TableMeta sourceTable) throws ModelException {
      try {
        ColumnMeta column = sourceTable.column(ident);
        Object value = column.getType().fromString(this.value);
        return new ColumnMeta(column.getType()) {

          @Override
          public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
            return value;
          }

          @Override
          public String print(Object[] values, Resources resources, TableContext context) throws IOException {
            return null;
          }

          @Override
          public TableDependencies tableDependencies() {
            return new TableDependencies();
          }

          @Override
          public MetaResources readResources() {
            return MetaResources.empty();
          }
        };
      } catch (RuleException e) {
        throw new ModelException(e.getMessage(), new Loc(0, 0));
      }
    }

    @Override
    public String print() {
      return null;
    }

    @Override
    public Loc loc() {
      return null;
    }
  }

  @Data
  public static class ValueField {
    private final String name;
    private final String value;

    public BinaryExpression toFilterExpr() {
      Ident ident = new Ident(name, new Loc(0, 0));
      return new BinaryExpression(
          new Token(String.valueOf(Tokens.EQ), new Loc(0, 0)),
          FuncCallExpression.of(ident),
          new Constant(ident, value));
    }

    public SetExpression toSetExpr() {
      Ident ident = new Ident(name, new Loc(0, 0));
      return new SetExpression(ident, new Constant(ident, value));
    }
  }
}
