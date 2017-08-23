package com.cosyan.db.sql;

import java.io.IOException;

import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class CreateStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Create extends Node implements Statement {
    private final String name;
    private final ImmutableList<ColumnDefinition> columns;

    @Override
    public boolean execute(MetaRepo metaRepo) throws ModelException, IOException {
      ImmutableMap.Builder<String, BasicColumn> columnsBuilder = ImmutableMap.builder();
      int i = 0;
      for (ColumnDefinition column : columns) {
        BasicColumn basicColumn = new BasicColumn(i++, column.getName(), column.getType(), column.isNullable(), column.isUnique());
        if (column.isUnique()) {
          if (column.getType() != DataTypes.StringType && column.getType() != DataTypes.LongType) {
            throw new ModelException("Unique indexes are only supported for " + DataTypes.StringType +
                " and " + DataTypes.LongType + " types, not " + column.getType() + ".");
          } else {
            metaRepo.registerIndex(name + "." + column.getName(), basicColumn);
          }
        }
        columnsBuilder.put(column.getName(), basicColumn);
      }
      MaterializedTableMeta tableMeta = new MaterializedTableMeta(
          name, columnsBuilder.build(), metaRepo);
      metaRepo.registerTable(name, tableMeta);
      return true;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ColumnDefinition extends Node {
    private final String name;
    private final DataType<?> type;
    private final boolean nullable;
    private final boolean unique;
  }
}
