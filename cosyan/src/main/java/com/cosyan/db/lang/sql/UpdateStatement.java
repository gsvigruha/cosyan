package com.cosyan.db.lang.sql;

import java.io.IOException;
import java.util.Optional;

import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.sql.Result.StatementResult;
import com.cosyan.db.lang.sql.SyntaxTree.Expression;
import com.cosyan.db.lang.sql.SyntaxTree.Ident;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.lang.sql.SyntaxTree.Statement;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class UpdateStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class SetExpression extends Node {
    private final Ident ident;
    private final Expression value;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Update extends Node implements Statement {
    private final Ident table;
    private final ImmutableList<SetExpression> updates;
    private final Optional<Expression> where;

    private DerivedColumn whereColumn;
    private ImmutableMap<Integer, DerivedColumn> columnExprs;

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      MaterializedTableMeta tableMeta = (MaterializedTableMeta) metaRepo.table(table);
      ImmutableMap.Builder<Integer, DerivedColumn> columnExprsBuilder = ImmutableMap.builder();
      for (SetExpression update : updates) {
        int idx = tableMeta.indexOf(update.getIdent());
        if (idx < 0) {
          throw new ModelException("Identifier '" + update.getIdent() + "' not found.");
        }
        DerivedColumn columnExpr = update.getValue().compile(tableMeta);
        columnExprsBuilder.put(idx, columnExpr);
      }
      columnExprs = columnExprsBuilder.build();
      if (where.isPresent()) {
        whereColumn = where.get().compile(tableMeta);
      } else {
        whereColumn = ColumnMeta.TRUE_COLUMN;
      }
      return MetaResources.updateTable(tableMeta);
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      TableWriter writer = resources.writer(table);
      return new StatementResult(writer.update(columnExprs, whereColumn));
    }

    @Override
    public void cancel() {

    }
  }
}
