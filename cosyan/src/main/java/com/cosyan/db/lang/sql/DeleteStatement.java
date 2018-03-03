package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.sql.Result.StatementResult;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.lang.sql.SyntaxTree.Statement;
import com.cosyan.db.logic.PredicateHelper;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.TableProvider;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.MaterializedTableMeta.SeekableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DeleteStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Delete extends Node implements Statement {
    private final Ident table;
    private final Expression where;

    private SeekableTableMeta tableMeta;
    private ColumnMeta whereColumn;
    private VariableEquals clause;

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      TableWriter writer = resources.writer(tableMeta.tableName());
      if (clause == null) {
        return new StatementResult(writer.delete(resources, whereColumn));
      } else {
        return new StatementResult(writer.deleteWithIndex(resources, whereColumn, clause));
      }
    }

    @Override
    public void cancel() {

    }

    @Override
    public MetaResources compile(TableProvider tableProvider) throws ModelException {
      MaterializedTableMeta materializedTableMeta = tableProvider.table(table);
      tableMeta = materializedTableMeta.reader();
      whereColumn = where.compileColumn(tableMeta);
      clause = PredicateHelper.getBestClause(tableMeta, where);
      return MetaResources.deleteFromTable(materializedTableMeta);
    }
  }
}
