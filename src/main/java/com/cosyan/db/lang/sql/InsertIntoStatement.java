package com.cosyan.db.lang.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Literals.Literal;
import com.cosyan.db.lang.expr.Statements.Statement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.InsertIntoResult;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaReader;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableUniqueIndex.IDTableIndex;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class InsertIntoStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class InsertInto extends Statement {
    private final Ident table;
    private final Optional<ImmutableList<Ident>> columns;
    private final ImmutableList<ImmutableList<Literal>> valuess;

    private MaterializedTable tableMeta;
    private ImmutableMap<Ident, Integer> indexes;

    @Override
    public MetaResources compile(MetaReader metaRepo) throws ModelException {
      tableMeta = metaRepo.table(table);
      ImmutableMap.Builder<Ident, Integer> indexesBuilder = ImmutableMap.builder();
      if (columns.isPresent()) {
        for (int i = 0; i < columns.get().size(); i++) {
          Ident ident = columns.get().get(i);
          BasicColumn column = tableMeta.column(ident);
          if (column.getType() == null) {
            throw new ModelException(
                String.format("Cannot specify value for ID type column '%s' directly.", column.getName()), ident);
          }
          indexesBuilder.put(ident, column.getIndex());
        }
      }
      indexes = indexesBuilder.build();
      return MetaResources.insertIntoTable(tableMeta);
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      ImmutableList<BasicColumn> cols = ImmutableList.copyOf(tableMeta.columns().values());
      boolean hasID = cols.get(0).getType() == DataTypes.IDType;
      Object[] fullValues = new Object[cols.size()];

      long lastID = hasID ? ((IDTableIndex) resources.getPrimaryKeyIndex(table.getString())).getLastID() : -1;
      TableWriter writer = resources.writer(tableMeta.tableName());
      List<Long> newIDs = new ArrayList<>();
      for (ImmutableList<Literal> values : valuess) {
        if (columns.isPresent()) {
          Arrays.fill(fullValues, null);
          if (hasID) {
            fullValues[0] = ++lastID;
            newIDs.add(lastID);
          }
          for (int i = 0; i < columns.get().size(); i++) {
            int idx = indexes.get(columns.get().get(i));
            fullValues[idx] = values.get(i).getValue();
          }
        } else {
          int offset = 0;
          if (hasID) {
            fullValues[0] = ++lastID;
            newIDs.add(lastID);
            offset = 1;
          }
          if (values.size() + offset != fullValues.length) {
            throw new RuleException(
                String.format("Expected '%s' values but got '%s'.", fullValues.length - offset, values.size()));
          }
          for (int i = offset; i < fullValues.length; i++) {
            fullValues[i] = values.get(i - offset).getValue();
          }
        }
        writer.insert(resources, fullValues, /* checkReferencingRules= */true);
      }
      tableMeta.insert(valuess.size());
      return new InsertIntoResult(valuess.size(), newIDs);
    }

    @Override
    public void cancel() {
    }
  }
}
