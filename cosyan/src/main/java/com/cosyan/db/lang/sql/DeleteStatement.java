package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.sql.Result.StatementResult;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.lang.sql.SyntaxTree.Statement;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
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

    private MaterializedTableMeta tableMeta;
    private ColumnMeta whereColumn;

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      TableWriter writer = tableMeta.writer(resources);
      return new StatementResult(writer.delete(whereColumn));
    }

    @Override
    public void cancel() {

    }

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      tableMeta = (MaterializedTableMeta) metaRepo.table(table);
      whereColumn = where.compileColumn(tableMeta);
      return MetaResources.deleteFromTable(tableMeta);
    }
  }
}
