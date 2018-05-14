package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.io.Indexes.IndexWriter;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.SyntaxTree.AlterStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.expr.TableDefinition.ForeignKeyDefinition;
import com.cosyan.db.lang.expr.TableDefinition.RuleDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.MetaRepoExecutor;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatementConstraints {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddForeignKey extends Node implements AlterStatement {
    private final Ident table;
    private final ForeignKeyDefinition constraint;

    private ForeignKey foreignKey;
    private TableWriter writer;

    @Override
    public MetaResources compile(MetaRepo metaRepo, AuthToken authToken) throws ModelException {
      MaterializedTable tableMeta = metaRepo.table(table);
      MaterializedTable refTable = metaRepo.table(constraint.getRefTable());
      foreignKey = tableMeta.createForeignKey(constraint, refTable);
      return MetaResources.tableMeta(tableMeta).merge(MetaResources.tableMeta(refTable));
    }

    @Override
    public boolean log() {
      return true;
    }

    @Override
    public Result execute(MetaRepoExecutor metaRepo, Resources resources) throws RuleException, IOException {
      MaterializedTable tableMeta = resources.meta(table.getString());
      IndexWriter indexWriter = metaRepo.registerIndex(tableMeta, foreignKey.getColumn());
      writer = resources.writer(table.getString());
      writer.checkForeignKey(foreignKey, resources);
      String colName = foreignKey.getColumn().getName();
      writer.buildIndex(colName, indexWriter);
      tableMeta.addForeignKey(foreignKey);
      metaRepo.syncMeta(tableMeta);
      return Result.META_OK;
    }

    @Override
    public void cancel() {
      writer.cancel();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddRule extends Node implements AlterStatement {
    private final Ident table;
    private final RuleDefinition constraint;

    private BooleanRule rule;
    private TableWriter writer;

    @Override
    public MetaResources compile(MetaRepo metaRepo, AuthToken authToken) throws ModelException {
      MaterializedTable tableMeta = metaRepo.table(table);
      rule = tableMeta.createRule(constraint);
      return MetaResources.tableMeta(tableMeta).merge(rule.getColumn().readResources());
    }

    @Override
    public boolean log() {
      return true;
    }

    @Override
    public Result execute(MetaRepoExecutor metaRepo, Resources resources) throws RuleException, IOException {
      MaterializedTable tableMeta = resources.meta(table.getString());
      writer = resources.writer(table.getString());
      writer.checkRule(rule, resources);
      tableMeta.addRule(rule);
      metaRepo.syncMeta(tableMeta);
      return Result.META_OK;
    }

    @Override
    public void cancel() {
      writer.cancel();
    }
  }
}
