package com.cosyan.db.sql;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import com.cosyan.db.io.TableWriter.TableAppender;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
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
    private final ImmutableList<Literal> values;

    @Override
    public boolean execute(MetaRepo metaRepo) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = (MaterializedTableMeta) metaRepo.table(table);
      Object[] fullValues = new Object[tableMeta.columns().size()];
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
      TableAppender appender = tableMeta.appender();
      appender.write(fullValues);
      appender.close();
      return true;
    }
  }
}
