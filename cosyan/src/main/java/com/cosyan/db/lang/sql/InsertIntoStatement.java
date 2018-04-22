package com.cosyan.db.lang.sql;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Literals.Literal;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.StatementResult;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.TableUniqueIndex.IDTableIndex;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class InsertIntoStatement {

  public static Object check(DataType<?> dataType, Literal literal) throws RuleException {
    if (literal.getValue() != DataTypes.NULL && literal.getType() != dataType) {
      throw new RuleException(String.format("Expected '%s' but got '%s'.", dataType, literal.getType()));
    }
    return literal.getValue();
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class InsertInto extends Node implements Statement {
    private final Ident table;
    private final Optional<ImmutableList<Ident>> columns;
    private final ImmutableList<ImmutableList<Literal>> valuess;

    private MaterializedTableMeta tableMeta;
    private ImmutableMap<Ident, Integer> indexes;

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      tableMeta = metaRepo.table(table);
      ImmutableMap.Builder<Ident, Integer> indexesBuilder = ImmutableMap.builder();
      if (columns.isPresent()) {
        for (int i = 0; i < columns.get().size(); i++) {
          Ident ident = columns.get().get(i);
          BasicColumn column = tableMeta.column(ident);
          if (column.getType() == DataTypes.NULL) {
            throw new ModelException(
                String.format("Cannot specify value for ID type column '%s' directly.", column.getName()), ident);
          }
          indexesBuilder.put(ident, column.getIndex());
        }
      }
      indexes = indexesBuilder.build();
      return MetaResources
          .insertIntoTable(tableMeta)
          .merge(tableMeta.ruleDependenciesReadResources())
          .merge(tableMeta.reverseRuleDependenciesReadResources());
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      ImmutableList<BasicColumn> cols = ImmutableList.copyOf(tableMeta.columns().values());
      boolean hasID = cols.get(0).getType() == DataTypes.IDType;
      Object[] fullValues = new Object[cols.size()];

      long lastID = hasID ? ((IDTableIndex) resources.getPrimaryKeyIndex(table.getString())).getLastID() : -1;
      TableWriter writer = resources.writer(tableMeta.tableName());
      for (ImmutableList<Literal> values : valuess) {
        if (columns.isPresent()) {
          Arrays.fill(fullValues, DataTypes.NULL);
          if (hasID) {
            fullValues[0] = ++lastID;
          }
          for (int i = 0; i < columns.get().size(); i++) {
            int idx = indexes.get(columns.get().get(i));
            fullValues[idx] = check(cols.get(idx).getType(), values.get(i));
          }
        } else {
          int offset = 0;
          if (hasID) {
            fullValues[0] = ++lastID;
            offset = 1;
          }
          if (values.size() + offset != fullValues.length) {
            throw new RuleException(
                String.format("Expected '%s' values but got '%s'.", fullValues.length - offset, values.size()));
          }
          for (int i = offset; i < fullValues.length; i++) {
            fullValues[i] = check(cols.get(i).getType(), values.get(i - offset));
          }
        }
        writer.insert(resources, fullValues, /* checkReferencingRules= */true);
      }
      tableMeta.insert(valuess.size());
      return new StatementResult(valuess.size());
    }

    @Override
    public void cancel() {

    }
  }
}
