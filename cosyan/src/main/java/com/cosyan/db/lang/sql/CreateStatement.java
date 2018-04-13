package com.cosyan.db.lang.sql;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.MetaStatementResult;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.MaterializedTableMeta.SeekableTableMeta;
import com.cosyan.db.model.Rule;
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
    private final MaterializedTableMeta.Type type;
    private final ImmutableList<ColumnDefinition> columnDefinitions;
    private final ImmutableList<ConstraintDefinition> constraints;
    private final Optional<Expression> partitioning;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IOException {
      if (metaRepo.hasTable(name)) {
        throw new ModelException(String.format("Table '%s' already exists.", name));
      }

      Optional<PrimaryKey> primaryKey = Optional.empty();
      LinkedHashMap<String, BasicColumn> columns = Maps.newLinkedHashMap();
      int i = 0;
      for (ColumnDefinition column : columnDefinitions) {
        BasicColumn basicColumn = new BasicColumn(
            i,
            column.getName(),
            column.getType(),
            column.isNullable(),
            column.isUnique(),
            /* indexed= */column.isUnique(),
            column.isImmutable() || column.getType() == DataTypes.IDType);
        if (column.getType() == DataTypes.IDType) {
          if (i != 0) {
            throw new ModelException(String.format("The ID column '%s' has to be the first one.", column.getName()));
          }
          basicColumn.setNullable(false);
          basicColumn.setUnique(true);
          basicColumn.setIndexed(true);
          primaryKey = Optional.of(new PrimaryKey("id", basicColumn));
        }
        columns.put(column.getName(), basicColumn);
        i++;
      }

      for (BasicColumn column : columns.values()) {
        if (column.isUnique()) {
          if (column.getType() != DataTypes.StringType
              && column.getType() != DataTypes.LongType
              && column.getType() != DataTypes.IDType) {
            throw new ModelException("Unique indexes are only supported for " + DataTypes.StringType +
                ", " + DataTypes.LongType + " and " + DataTypes.IDType + " types, not " + column.getType() + ".");
          }
        }
      }

      for (ConstraintDefinition constraint : constraints) {
        if (constraint instanceof PrimaryKeyDefinition) {
          if (!primaryKey.isPresent()) {
            PrimaryKeyDefinition primaryKeyDefinition = (PrimaryKeyDefinition) constraint;
            String pkColumnName = primaryKeyDefinition.getKeyColumn().getString();
            if (!columns.containsKey(pkColumnName)) {
              throw new ModelException(
                  String.format("Invalid primary key definition: column '%s' not found.", pkColumnName));
            }
            BasicColumn keyColumn = columns.get(pkColumnName);
            keyColumn.setNullable(false);
            keyColumn.setUnique(true);
            keyColumn.setIndexed(true);
            primaryKey = Optional.of(new PrimaryKey(primaryKeyDefinition.getName(), keyColumn));
          } else {
            throw new ModelException("There can only be one primary key.");
          }
        }
      }
      MaterializedTableMeta tableMeta = new MaterializedTableMeta(
          metaRepo.config(),
          name,
          authToken.username(),
          columns.values(),
          primaryKey,
          type);
      addConstraints(metaRepo, tableMeta, constraints);

      for (BasicColumn column : columns.values()) {
        if (column.isIndexed()) {
          if (column.isUnique()) {
            metaRepo.registerUniqueIndex(tableMeta, column);
          } else {
            metaRepo.registerMultiIndex(tableMeta, column);
          }
        }
      }

      if (partitioning.isPresent()) {
        ColumnMeta columnMeta = partitioning.get().compileColumn(tableMeta.reader());
        tableMeta.setPartitioning(Optional.of(columnMeta));
      }

      metaRepo.registerTable(tableMeta);
      return new MetaStatementResult();
    }

    public static void addConstraints(
        MetaRepo metaRepo,
        MaterializedTableMeta tableMeta,
        List<ConstraintDefinition> constraints)
        throws ModelException {
      List<RuleDefinition> ruleDefinitions = Lists.newArrayList();
      List<ForeignKeyDefinition> foreignKeyDefinitions = Lists.newArrayList();
      for (ConstraintDefinition constraint : constraints) {
        if (constraint instanceof RuleDefinition) {
          RuleDefinition ruleDefinition = (RuleDefinition) constraint;
          ruleDefinitions.add(ruleDefinition);
        } else if (constraint instanceof ForeignKeyDefinition) {
          ForeignKeyDefinition foreignKey = (ForeignKeyDefinition) constraint;
          foreignKeyDefinitions.add(foreignKey);
        } else if (constraint instanceof PrimaryKeyDefinition) {
          // Pass.
        } else {
          throw new ModelException(String.format("Invalid constraint %s.", constraint));
        }
      }

      for (ForeignKeyDefinition foreignKeyDefinition : foreignKeyDefinitions) {
        BasicColumn keyColumn = tableMeta.column(foreignKeyDefinition.getKeyColumn());
        MaterializedTableMeta refTable = metaRepo.table(foreignKeyDefinition.getRefTable());
        BasicColumn refColumn = refTable.pkColumn();
        if (foreignKeyDefinition.getRefColumn().isPresent()
            && !foreignKeyDefinition.getRefColumn().get().getString().equals(refColumn.getName())) {
          throw new ModelException(
              "Foreign key reference column has to be the primary key column of the referenced table.");
        }
        if (keyColumn.getType() != refColumn.getType()
            && !(keyColumn.getType() == DataTypes.LongType && refColumn.getType() == DataTypes.IDType)) {
          throw new ModelException(
              String.format("Foreign key reference column has type '%s' while key column has type '%s'.",
                  refColumn.getType(), keyColumn.getType()));
        }
        assert refColumn.isUnique() && !refColumn.isNullable();
        // Unique keys are indexed by default, so no need to change refColumn.
        keyColumn.setIndexed(true);
        tableMeta.addForeignKey(new ForeignKey(
            foreignKeyDefinition.getName(),
            foreignKeyDefinition.getRevName(),
            tableMeta,
            keyColumn,
            refTable,
            refColumn));
      }

      for (RuleDefinition ruleDefinition : ruleDefinitions) {
        Rule rule = ruleDefinition.compile(tableMeta);
        if (rule.getType() != DataTypes.BoolType) {
          throw new ModelException("Constraint expression has to be boolean.");
        }
        tableMeta.addRule(rule.toBooleanRule());
      }
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ColumnDefinition extends Node {
    private final String name;
    private final DataType<?> type;
    private final boolean nullable;
    private final boolean unique;
    private final boolean immutable;
  }

  public interface ConstraintDefinition {
    public String getName();
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class RuleDefinition extends Node implements ConstraintDefinition {
    private final String name;
    private final Expression expr;
    private final boolean nullIsTrue;

    @Override
    public String toString() {
      return name + " [" + expr.print() + "]";
    }

    public Rule compile(MaterializedTableMeta tableMeta) throws ModelException {
      SeekableTableMeta table = tableMeta.reader();
      ColumnMeta column = expr.compileColumn(table);
      return new Rule(name, table, column, expr, nullIsTrue, column.tableDependencies());
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
    private final String revName;
    private final Ident keyColumn;
    private final Ident refTable;
    private final Optional<Ident> refColumn;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class RefDefinition extends Node {
    private final String name;
    private final Select select;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CreateIndex extends Node implements MetaStatement {

    private final Ident table;
    private final Ident column;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IndexException, IOException {
      MaterializedTableMeta tableMeta = metaRepo.table(table);
      BasicColumn column = tableMeta.column(this.column);
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
