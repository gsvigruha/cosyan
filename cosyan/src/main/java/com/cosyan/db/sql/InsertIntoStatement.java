package com.cosyan.db.sql;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import com.cosyan.db.io.TableWriter;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.sql.Result.StatementResult;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.Literal;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.Statement;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
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

    private MaterializedTableMeta tableMeta;

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      tableMeta = (MaterializedTableMeta) metaRepo.table(table);
      return MetaResources.insertIntoTable(tableMeta);
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      Object[] fullValues = new Object[tableMeta.columns().size()];
      TableWriter writer = resources.writer(table);
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
            throw new RuleException("Expected '" + fullValues.length + "' values but got '" + values.size() + "'.");
          }
          for (int i = 0; i < values.size(); i++) {
            fullValues[i] = values.get(i).getValue();
          }
        }
        writer.insert(fullValues);
      }
      return new StatementResult(valuess.size());
    }

    @Override
    public void cancel() {

    }
  }
}
