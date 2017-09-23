package com.cosyan.db.sql;

import java.io.IOException;
import java.util.List;

import com.cosyan.db.io.TableWriter.TableDeleter;
import com.cosyan.db.lock.ResourceLock;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.Result.StatementResult;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.Statement;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DeleteStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Delete extends Node implements Statement {
    private final Ident table;
    private final Expression where;

    private TableDeleter deleter;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = (MaterializedTableMeta) metaRepo.table(table);
      DerivedColumn whereColumn = where.compile(tableMeta, metaRepo);
      deleter = tableMeta.deleter(whereColumn);
      return new StatementResult(deleter.delete());
    }

    @Override
    public void rollback() {
      if (deleter != null) {
        deleter.rollback();
      }
    }

    @Override
    public void commit() throws IOException {
      if (deleter != null) {
        deleter.commit();
        deleter.close();
      }
    }

    @Override
    public void collectLocks(List<ResourceLock> locks) {
      locks.add(ResourceLock.readWrite(table));
    }
  }
}
