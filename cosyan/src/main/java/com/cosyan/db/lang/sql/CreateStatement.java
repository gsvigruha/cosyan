package com.cosyan.db.lang.sql;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.lang.sql.Result.MetaStatementResult;
import com.cosyan.db.lang.sql.SyntaxTree.Expression;
import com.cosyan.db.lang.sql.SyntaxTree.Ident;
import com.cosyan.db.lang.sql.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.MaterializedTableMeta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class CreateStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CreateTable extends Node implements MetaStatement {
    private final String name;
    private final ImmutableList<ColumnDefinition> columnDefinitions;
    private final ImmutableList<ConstraintDefinition> constraints;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException {
      if (metaRepo.hasTable(name)) {
        throw new ModelException(String.format("Table '%s' already exists.", name));
      }

      LinkedHashMap<String, BasicColumn> columns = Maps.newLinkedHashMap();
      int i = 0;
      for (ColumnDefinition column : columnDefinitions) {
        BasicColumn basicColumn = new BasicColumn(
            i++,
            column.getName(),
            column.getType(),
            column.isNullable(),
            column.isUnique());
        columns.put(column.getName(), basicColumn);
      }

      for (BasicColumn column : columns.values()) {
        if (column.isUnique()) {
          if (column.getType() != DataTypes.StringType && column.getType() != DataTypes.LongType) {
            throw new ModelException("Unique indexes are only supported for " + DataTypes.StringType +
                " and " + DataTypes.LongType + " types, not " + column.getType() + ".");
          }
        }
      }

      List<SimpleCheckDefinition> simpleCheckDefinition = Lists.newArrayList();
      Map<String, DerivedColumn> simpleChecks = Maps.newHashMap();
      Optional<PrimaryKey> primaryKey = Optional.empty();
      List<ForeignKey> foreignKeys = Lists.newArrayList();
      for (ConstraintDefinition constraint : constraints) {
        if (columns.containsKey(constraint.getName())) {
          throw new ModelException("Name collision for constraint '" + constraint.getName() + "'.");
        }
        if (constraint instanceof SimpleCheckDefinition) {
          SimpleCheckDefinition simpleCheck = (SimpleCheckDefinition) constraint;
          DerivedColumn constraintColumn = simpleCheck.getExpr().compile(
              MaterializedTableMeta.simpleTable(name, columns.values()));
          if (constraintColumn.getType() != DataTypes.BoolType) {
            throw new ModelException("Constraint expression has to be boolean.");
          }
          simpleChecks.put(simpleCheck.getName(), constraintColumn);
          simpleCheckDefinition.add(simpleCheck);
        } else if (constraint instanceof PrimaryKeyDefinition) {
          if (!primaryKey.isPresent()) {
            PrimaryKeyDefinition primaryKeyDefinition = (PrimaryKeyDefinition) constraint;
            BasicColumn keyColumn = columns.get(primaryKeyDefinition.getKeyColumn().getString());
            keyColumn.setNullable(false);
            keyColumn.setUnique(true);
            keyColumn.setIndexed(true);
            primaryKey = Optional.of(new PrimaryKey(primaryKeyDefinition.getName(), keyColumn));
          } else {
            throw new ModelException("There can only be one primary key.");
          }
        } else if (constraint instanceof ForeignKeyDefinition) {
          ForeignKeyDefinition foreignKey = (ForeignKeyDefinition) constraint;
          BasicColumn keyColumn = columns.get(foreignKey.getKeyColumn().getString());
          MaterializedTableMeta refTable = metaRepo.table(foreignKey.getRefTable());
          BasicColumn refColumn = refTable.columns().get(foreignKey.getRefColumn().getString());
          if (!refColumn.isUnique()) {
            throw new ModelException("Foreign key reference column has to be unique.");
          }
          // Unique keys are indexed by default.
          keyColumn.setIndexed(true);
          foreignKeys.add(
              new ForeignKey(foreignKey.getName(), keyColumn, refTable, refColumn));
        }
      }

      MaterializedTableMeta tableMeta = new MaterializedTableMeta(
          name,
          columns.values(),
          simpleCheckDefinition,
          simpleChecks,
          primaryKey);
      for (ForeignKey foreignKey : foreignKeys) {
        tableMeta.addForeignKey(foreignKey);
      }

      for (BasicColumn column : columns.values()) {
        if (column.isIndexed()) {
          if (column.isUnique()) {
            metaRepo.registerUniqueIndex(tableMeta, column);
          } else {
            metaRepo.registerMultiIndex(tableMeta, column);
          }
        }
      }

      metaRepo.registerTable(name, tableMeta);
      return new MetaStatementResult();
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

  public interface ConstraintDefinition {
    public String getName();
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class SimpleCheckDefinition extends Node implements ConstraintDefinition {
    private final String name;
    private final Expression expr;

    @Override
    public String toString() {
      return name + " [" + expr.print() + "]";
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class PrimaryKeyDefinition extends Node implements ConstraintDefinition {
    private final String name;
    private final Ident keyColumn;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ForeignKeyDefinition extends Node implements ConstraintDefinition {
    private final String name;
    private final Ident keyColumn;
    private final Ident refTable;
    private final Ident refColumn;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CreateIndex extends Node implements MetaStatement {

    private final Ident ident;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IndexException, IOException {
      MaterializedTableMeta tableMeta = metaRepo.table(new Ident(ident.head()));
      BasicColumn column = tableMeta.column(ident.tail());
      if (column.isIndexed()) {
        throw new ModelException(String.format("Cannot create index on '%s.%s', column is already indexed.",
            tableMeta.tableName(), column.getName()));
      }
      metaRepo.registerMultiIndex(tableMeta, column);
      column.setIndexed(true);
      return new MetaStatementResult();
    }
  }
}
