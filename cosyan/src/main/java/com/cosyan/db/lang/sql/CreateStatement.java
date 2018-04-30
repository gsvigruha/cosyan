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
import com.cosyan.db.meta.MaterializedTableMeta;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MaterializedTableMeta.SeekableTableMeta;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
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
    private final Ident name;
    private final MaterializedTableMeta.Type type;
    private final ImmutableList<ColumnDefinition> columnDefinitions;
    private final ImmutableList<ConstraintDefinition> constraints;
    private final Optional<Expression> partitioning;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IOException {
      if (metaRepo.hasTable(name.getString())) {
        throw new ModelException(String.format("Table '%s' already exists.", name), name);
      }

      Optional<PrimaryKeyDefinition> primaryKeyDefinition = Optional.empty();
      for (ConstraintDefinition constraint : constraints) {
        if (constraint instanceof PrimaryKeyDefinition) {
          primaryKeyDefinition = Optional.of((PrimaryKeyDefinition) constraint);
        }
      }

      LinkedHashMap<Ident, BasicColumn> columns = Maps.newLinkedHashMap();
      Optional<PrimaryKey> primaryKey = Optional.empty();
      int i = 0;
      for (ColumnDefinition column : columnDefinitions) {
        boolean isPK = primaryKeyDefinition.map(pk -> pk.getKeyColumn().equals(column.getName())).orElse(false);
        boolean isID = column.getType() == DataTypes.IDType;
        BasicColumn basicColumn = new BasicColumn(
            i,
            column.getName(),
            column.getType(),
            column.isNullable() && !(isPK || isID),
            column.isUnique() || (isPK || isID),
            column.isImmutable() || (isPK || isID));
        if (isPK) {
          if (primaryKey.isPresent()) {
            throw new ModelException("There can only be one primary key.", primaryKeyDefinition.get().getName());
          }
          primaryKey = Optional.of(new PrimaryKey(primaryKeyDefinition.get().getName(), basicColumn));
        } else if (isID) {
          if (primaryKey.isPresent()) {
            throw new ModelException("There can only be one primary key.", basicColumn.getIdent());
          }
          primaryKey = Optional.of(new PrimaryKey(new Ident("pk_id"), basicColumn));
        }
        columns.put(column.getName(), basicColumn);
        i++;
      }

      if (primaryKeyDefinition.isPresent() && !primaryKey.isPresent()) {
        throw new ModelException(
            String.format("Invalid primary key definition: column '%s' not found.",
                primaryKeyDefinition.get().getName()),
            primaryKeyDefinition.get().getName());
      }

      MaterializedTableMeta tableMeta = new MaterializedTableMeta(
          metaRepo.config(),
          name.getString(),
          authToken.username(),
          columns.values(),
          primaryKey,
          type);

      addConstraints(metaRepo, tableMeta, constraints);

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
        throws ModelException, IOException {
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
          throw new ModelException(String.format("Invalid constraint %s.", constraint), constraint.getName());
        }
      }

      for (ForeignKeyDefinition foreignKeyDefinition : foreignKeyDefinitions) {
        BasicColumn keyColumn = tableMeta.column(foreignKeyDefinition.getKeyColumn());
        Ident refTableName = foreignKeyDefinition.getRefTable();
        MaterializedTableMeta refTable = metaRepo.table(refTableName);
        Optional<BasicColumn> refColumnOpt = refTable.pkColumn();
        if (!refColumnOpt.isPresent()) {
          throw new ModelException(String.format("Table '%s' has no primary key.", refTableName), refTableName);
        }
        BasicColumn refColumn = refColumnOpt.get();

        if (foreignKeyDefinition.getRefColumn().isPresent()
            && !foreignKeyDefinition.getRefColumn().get().getString().equals(refColumn.getName())) {
          throw new ModelException(
              "Foreign key reference column has to be the primary key column of the referenced table.",
              foreignKeyDefinition.refColumn.get());
        }
        if (keyColumn.getType() != refColumn.getType()
            && !(keyColumn.getType() == DataTypes.LongType && refColumn.getType() == DataTypes.IDType)) {
          throw new ModelException(
              String.format("Foreign key reference column has type '%s' while key column has type '%s'.",
                  refColumn.getType(), keyColumn.getType()),
              foreignKeyDefinition.getName());
        }
        assert refColumn.isUnique() && !refColumn.isNullable();
        keyColumn.addIndex(tableMeta);
        tableMeta.addForeignKey(foreignKeyDefinition.getName(), foreignKeyDefinition.getRevName(), new ForeignKey(
            foreignKeyDefinition.getName().getString(),
            foreignKeyDefinition.getRevName().getString(),
            tableMeta,
            keyColumn,
            refTable,
            refColumn));
      }

      for (RuleDefinition ruleDefinition : ruleDefinitions) {
        Rule rule = ruleDefinition.compile(tableMeta);
        if (rule.getType() != DataTypes.BoolType) {
          throw new ModelException("Constraint expression has to be boolean.", ruleDefinition.getName());
        }
        tableMeta.addRule(ruleDefinition.getName(), rule.toBooleanRule());
      }
    }

    @Override
    public boolean log() {
      return true;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ColumnDefinition extends Node {
    private final Ident name;
    private final DataType<?> type;
    private final boolean nullable;
    private final boolean unique;
    private final boolean immutable;
  }

  public interface ConstraintDefinition {
    public Ident getName();
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class RuleDefinition extends Node implements ConstraintDefinition {
    private final Ident name;
    private final Expression expr;
    private final boolean nullIsTrue;

    @Override
    public String toString() {
      return name + " [" + expr.print() + "]";
    }

    public Rule compile(MaterializedTableMeta tableMeta) throws ModelException {
      SeekableTableMeta table = tableMeta.reader();
      ColumnMeta column = expr.compileColumn(table);
      return new Rule(name.getString(), table, column, expr, nullIsTrue, column.tableDependencies());
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class PrimaryKeyDefinition extends Node implements ConstraintDefinition {
    private final Ident name;
    private final Ident keyColumn;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ForeignKeyDefinition extends Node implements ConstraintDefinition {
    private final Ident name;
    private final Ident revName;
    private final Ident keyColumn;
    private final Ident refTable;
    private final Optional<Ident> refColumn;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class RefDefinition extends Node {
    private final Ident name;
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
      column.addIndex(tableMeta);
      metaRepo.sync(tableMeta);
      return new MetaStatementResult();
    }

    @Override
    public boolean log() {
      return true;
    }
  }
}
