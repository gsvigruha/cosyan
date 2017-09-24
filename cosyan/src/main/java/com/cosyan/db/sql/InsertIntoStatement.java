package com.cosyan.db.sql;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.io.TableWriter.TableAppender;
import com.cosyan.db.lock.ResourceLock;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.Result.StatementResult;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.Literal;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.Statement;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class InsertIntoStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class InsertInto extends Node implements Statement {
    private final Ident table;
    private final Optional<ImmutableList<Ident>> columns;
    private final ImmutableList<ImmutableList<Literal>> valuess;

    private TableAppender appender;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException, IndexException {
      MaterializedTableMeta tableMeta = (MaterializedTableMeta) metaRepo.table(table);
      Object[] fullValues = new Object[tableMeta.columns().size()];
      appender = tableMeta.appender();
      appender.init();
      for (ImmutableList<Literal> values : valuess) {
        if (columns.isPresent()) {
          Arrays.fill(fullValues, DataTypes.NULL);
          for (int i = 0; i < columns.get().size(); i++) {
            int idx = tableMeta.indexOf(columns.get().get(i));
            if (idx >= 0) {
              fullValues[idx] = values.get(i).getValue();
            }
          }
        } else {
          if (values.size() != fullValues.length) {
            throw new ModelException("Expected '" + fullValues.length + "' values but got '" + values.size() + "'.");
          }
          for (int i = 0; i < values.size(); i++) {
            fullValues[i] = values.get(i).getValue();
          }
        }
        appender.write(fullValues);
      }
      return new StatementResult(valuess.size());
    }

    @Override
    public void rollback() {
      if (appender != null) {
        appender.rollback();
      }
    }

    @Override
    public void commit() throws IOException {
      if (appender != null) {
        appender.commit();
        appender.close();
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
