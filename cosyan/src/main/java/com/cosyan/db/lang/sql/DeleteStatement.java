package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.StatementResult;
import com.cosyan.db.logic.PredicateHelper;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.SeekableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DeleteStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Delete extends Statement {
    private final Ident table;
    private final Expression where;

    private SeekableTableMeta tableMeta;
    private ColumnMeta whereColumn;
    private VariableEquals clause;

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      TableWriter writer = resources.writer(tableMeta.tableName());
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

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      MaterializedTable materializedTableMeta = metaRepo.table(table);
      tableMeta = materializedTableMeta.reader();
      whereColumn = where.compileColumn(tableMeta);
      clause = PredicateHelper.getBestClause(tableMeta, where);
      return MetaResources
          .deleteFromTable(materializedTableMeta)
          .merge(materializedTableMeta.reverseRuleDependenciesReadResources());
    }
  }
}
