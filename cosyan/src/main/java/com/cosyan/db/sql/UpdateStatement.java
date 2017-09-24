package com.cosyan.db.sql;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.io.TableWriter.TableUpdater;
import com.cosyan.db.lock.ResourceLock;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.Result.StatementResult;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.Statement;
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

    private TableUpdater updater;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException, IndexException {
      MaterializedTableMeta tableMeta = (MaterializedTableMeta) metaRepo.table(table);
      ImmutableMap.Builder<Integer, DerivedColumn> columnExprs = ImmutableMap.builder();
      for (SetExpression update : updates) {
        int idx = tableMeta.indexOf(update.getIdent());
        if (idx < 0) {
          throw new ModelException("Identifier '" + update.getIdent() + "' not found.");
        }
        DerivedColumn columnExpr = update.getValue().compile(tableMeta, metaRepo);
        columnExprs.put(idx, columnExpr);
      }
      DerivedColumn whereColumn;
      if (where.isPresent()) {
        whereColumn = where.get().compile(tableMeta, metaRepo);
      } else {
        whereColumn = ColumnMeta.TRUE_COLUMN;
      }
      updater = tableMeta.updater(columnExprs.build(), whereColumn);
      updater.init();
      return new StatementResult(updater.update());
    }

    @Override
    public void rollback() {
      if (updater != null) {
        updater.rollback();
      }
    }

    @Override
    public void commit() throws IOException {
      if (updater != null) {
        updater.commit();
        updater.close();
      }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void collectLocks(List<ResourceLock> locks) {
      locks.add(ResourceLock.readWrite(table));
    }
  }
}
