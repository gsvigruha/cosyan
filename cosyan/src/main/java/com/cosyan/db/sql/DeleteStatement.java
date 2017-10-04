package com.cosyan.db.sql;

import java.io.IOException;

import com.cosyan.db.io.TableWriter;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.MetaRepo.RuleException;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.Result.StatementResult;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.Statement;
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

    private DerivedColumn whereColumn;

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      TableWriter writer = resources.writer(table);
      return new StatementResult(writer.delete(whereColumn));
    }

    @Override
    public void cancel() {

    }

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      MaterializedTableMeta tableMeta = (MaterializedTableMeta) metaRepo.table(table);
      whereColumn = where.compile(tableMeta, metaRepo);
      return MetaResources.writeTable(tableMeta);
    }
  }
}
