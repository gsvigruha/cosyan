package com.cosyan.db.lang.sql;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.SyntaxTree.AlterStatement;
import com.cosyan.db.lang.expr.SyntaxTree.GlobalStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.expr.TableDefinition.ColumnDefinition;
import com.cosyan.db.lang.expr.TableDefinition.ConstraintDefinition;
import com.cosyan.db.lang.expr.TableDefinition.ForeignKeyDefinition;
import com.cosyan.db.lang.expr.TableDefinition.PrimaryKeyDefinition;
import com.cosyan.db.lang.expr.TableDefinition.RuleDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class CreateStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CreateTable extends Node implements GlobalStatement {
    private final Ident name;
    private final MaterializedTable.Type type;
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
        boolean isPK = primaryKeyDefinition.map(
            pk -> pk.getKeyColumn().getString().equals(column.getName().getString())).orElse(false);
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
            throw new ModelException("There can only be one primary key.", column.getName());
          }
          primaryKey = Optional.of(new PrimaryKey(new Ident("pk_id", name.getLoc()), basicColumn));
        }
        columns.put(column.getName(), basicColumn);
        i++;
      }

      if (primaryKeyDefinition.isPresent() && !primaryKey.isPresent()) {
        throw new ModelException(
            String.format("Invalid primary key definition '%s': column '%s' not found.",
                primaryKeyDefinition.get().getName(), primaryKeyDefinition.get().getKeyColumn()),
            primaryKeyDefinition.get().getName());
      }

      for (Entry<Ident, BasicColumn> column : columns.entrySet()) {
        if (column.getValue().getType() == DataTypes.IDType && column.getValue().getIndex() > 0) {
          throw new ModelException(String.format(
              "The ID column '%s' has to be the first one.", column.getValue().getName()), column.getKey());
        }
      }

      MaterializedTable tableMeta = new MaterializedTable(
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
      return Result.META_OK;
    }

    private void addConstraints(
        MetaRepo metaRepo,
        MaterializedTable tableMeta,
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
          throw new ModelException(String.format("Invalid constraint '%s'.", constraint), constraint.getName());
        }
      }

      for (ForeignKeyDefinition foreignKeyDefinition : foreignKeyDefinitions) {
        MaterializedTable refTable = metaRepo.table(foreignKeyDefinition.getRefTable());
        ForeignKey foreignKey = tableMeta.createForeignKey(foreignKeyDefinition, refTable);
        foreignKey.getColumn().setIndexed(true);
        tableMeta.addForeignKey(foreignKey);
      }

      for (RuleDefinition ruleDefinition : ruleDefinitions) {
        BooleanRule rule = tableMeta.createRule(ruleDefinition);
        tableMeta.addRule(rule);
      }
    }

    @Override
    public boolean log() {
      return true;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CreateIndex extends Node implements AlterStatement {

    private final Ident table;
    private final Ident column;

    private BasicColumn basicColumn;

    @Override
    public MetaResources compile(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IOException {
      MaterializedTable tableMeta = metaRepo.table(table);
      basicColumn = tableMeta.column(this.column);
      basicColumn.checkIndexType(this.column);
      basicColumn.setIndexed(true);
      metaRepo.syncIndex(tableMeta);
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public boolean log() {
      return true;
    }

    @Override
    public Result execute(MetaRepo metaRepo, Resources resources) throws RuleException, IOException {
      TableWriter writer = resources.writer(table.getString());
      writer.buildIndex(column.getString(), writer.getIndexWriter(column.getString()));
      return Result.META_OK;
    }
  }
}
