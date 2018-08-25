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

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Expression;
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
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.SeekableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DeleteStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Delete extends Statement {
    private final TableWithOwnerDefinition table;
    private final Expression where;

    private TableWithOwner tableWithOwner;
    private SeekableTableMeta tableMeta;
    private ColumnMeta whereColumn;
    private VariableEquals clause;

    @Override
    public MetaResources compile(MetaReader metaRepo, AuthToken authToken) throws ModelException {
      tableWithOwner = table.resolve(authToken);
      MaterializedTable materializedTableMeta = metaRepo.table(tableWithOwner);
      tableMeta = materializedTableMeta.reader();
      whereColumn = where.compileColumn(tableMeta);
      clause = PredicateHelper.getBestClause(tableMeta, where);
      return MetaResources.deleteFromTable(materializedTableMeta);
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      TableWriter writer = resources.writer(tableWithOwner.resourceId());
      long deletedLines;
      if (clause == null) {
        deletedLines = writer.delete(resources, whereColumn);
      } else {
        deletedLines = writer.deleteWithIndex(resources, whereColumn, clause);
      }
      tableMeta.tableMeta().delete(deletedLines);
      return new StatementResult(deletedLines);
    }

    @Override
    public void cancel() {
    }
  }
}
