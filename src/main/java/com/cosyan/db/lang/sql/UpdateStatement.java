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
package com.cosyan.db.lang.sql;

import java.io.IOException;
import java.util.Optional;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.Node;
import com.cosyan.db.lang.expr.Statements.Statement;
import com.cosyan.db.lang.expr.TableDefinition.TableWithOwnerDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.StatementResult;
import com.cosyan.db.logic.PredicateHelper;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaReader;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.TableProvider.TableWithOwner;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.SeekableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class UpdateStatement {

  private static void check(BasicColumn column, DataType<?> exprType, Ident ident) throws ModelException {
    DataType<?> columnType = column.getType();
    if (!exprType.isString() && !exprType.isNull() && !columnType.javaClass().equals(exprType.javaClass())) {
      throw new ModelException(String.format("Expected '%s' but got '%s' for '%s'.",
          columnType, exprType, column.getName()), ident);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class SetExpression extends Node {
    private final Ident ident;
    private final Expression value;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Update extends Statement {
    private final TableWithOwnerDefinition table;
    private final ImmutableList<SetExpression> updates;
    private final Optional<Expression> where;

    private TableWithOwner tableWithOwner;
    private SeekableTableMeta tableMeta;
    private ColumnMeta whereColumn;
    private ImmutableMap<Integer, ColumnMeta> columnExprs;
    private VariableEquals clause;

    @Override
    public MetaResources compile(MetaReader metaRepo, AuthToken authToken) throws ModelException {
      tableWithOwner = table.resolve(authToken);
      MaterializedTable materializedTableMeta = metaRepo.table(tableWithOwner);
      tableMeta = materializedTableMeta.meta();
      ImmutableMap.Builder<Integer, ColumnMeta> columnExprsBuilder = ImmutableMap.builder();
      for (SetExpression update : updates) {
        BasicColumn column = materializedTableMeta.column(update.getIdent());
        if (column.isImmutable()) {
          throw new ModelException(String.format(
              "Column '%s.%s' is immutable.", materializedTableMeta.fullName(), column.getName()), update.getIdent());
        }
        ColumnMeta columnExpr = update.getValue().compileColumn(tableMeta);
        check(column, columnExpr.getType(), update.getIdent());
        columnExprsBuilder.put(tableMeta.column(update.getIdent()).index(), columnExpr);
      }
      columnExprs = columnExprsBuilder.build();
      if (where.isPresent()) {
        whereColumn = where.get().compileColumn(tableMeta);
        clause = PredicateHelper.getBestClause(tableMeta, where.get());
      } else {
        whereColumn = ColumnMeta.TRUE_COLUMN;
      }
      return MetaResources.updateTable(materializedTableMeta);
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      // The rules must be re-evaluated for updated records. In addition, rules of
      // other tables referencing this table have to be re-evaluated as well. We need
      // the rule dependencies for the rules of this table and referencing rules.
      TableWriter writer = resources.writer(tableWithOwner.resourceId());
      if (clause == null) {
        return new StatementResult(writer.update(resources, columnExprs, whereColumn));
      } else {
        return new StatementResult(writer.updateWithIndex(resources, columnExprs, whereColumn, clause));
      }
    }

    @Override
    public void cancel() {
    }
  }
}
