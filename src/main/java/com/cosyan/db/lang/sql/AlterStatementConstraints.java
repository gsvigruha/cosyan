/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.io.Indexes.IndexWriter;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Statements.AlterStatement;
import com.cosyan.db.lang.expr.Statements.GlobalStatement;
import com.cosyan.db.lang.expr.TableDefinition.ForeignKeyDefinition;
import com.cosyan.db.lang.expr.TableDefinition.RuleDefinition;
import com.cosyan.db.lang.expr.TableDefinition.TableWithOwnerDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.MetaWriter;
import com.cosyan.db.meta.TableProvider.TableWithOwner;
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
  public static class AlterTableAddForeignKey extends AlterStatement {
    private final TableWithOwnerDefinition table;
    private final ForeignKeyDefinition constraint;

    private TableWithOwner tableWithOwner;
    private ForeignKey foreignKey;
    private TableWriter writer;
    private IndexWriter indexWriter;

    @Override
    public MetaResources executeMeta(MetaWriter metaRepo, AuthToken authToken) throws ModelException, IOException {
      tableWithOwner = table.resolve(authToken);
      MaterializedTable tableMeta = metaRepo.table(tableWithOwner);
      TableWithOwner refTableWithOwner = constraint.getRefTable().resolve(authToken);
      MaterializedTable refTable = metaRepo.table(refTableWithOwner);
      foreignKey = tableMeta.createForeignKey(constraint, refTable);
      indexWriter = metaRepo.registerIndex(tableMeta, foreignKey.getColumn());
      return MetaResources.tableMeta(tableMeta).merge(MetaResources.tableMeta(refTable));
    }

    @Override
    public Result executeData(MetaWriter metaRepo, Resources resources) throws RuleException, IOException {
      MaterializedTable tableMeta = resources.meta(tableWithOwner.resourceId());
      writer = resources.writer(tableWithOwner.resourceId());
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
  public static class AlterTableAddRule extends AlterStatement {
    private final TableWithOwnerDefinition table;
    private final RuleDefinition constraint;

    private TableWithOwner tableWithOwner;
    private BooleanRule rule;
    private TableWriter writer;

    @Override
    public MetaResources executeMeta(MetaWriter metaRepo, AuthToken authToken) throws ModelException {
      tableWithOwner = table.resolve(authToken);
      MaterializedTable tableMeta = metaRepo.table(tableWithOwner);
      rule = tableMeta.createRule(constraint);
      return MetaResources.tableMeta(tableMeta).merge(rule.getColumn().readResources());
    }

    @Override
    public Result executeData(MetaWriter metaRepo, Resources resources) throws RuleException, IOException {
      MaterializedTable tableMeta = resources.meta(tableWithOwner.resourceId());
      writer = resources.writer(tableWithOwner.resourceId());
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

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableDropConstraint extends GlobalStatement {
    private final TableWithOwnerDefinition table;
    private final Ident constraint;

    @Override
    public Result execute(MetaWriter metaRepo, AuthToken authToken) throws ModelException, GrantException, IOException {
      MaterializedTable tableMeta = metaRepo.table(table.resolve(authToken));
      if (tableMeta.hasRule(constraint.getString())) {
        tableMeta.dropRule(constraint.getString());
      } else if (tableMeta.hasForeignKey(constraint.getString())) {
        tableMeta.dropForeignKey(constraint, authToken);
      } else {
        throw new ModelException(String.format("Constraint '%s' not found in table '%s'.",
            constraint, table), constraint);
      }
      return Result.META_OK;
    }
  }
}
