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
import com.cosyan.db.lang.expr.Statements.AlterStatement;
import com.cosyan.db.lang.expr.TableDefinition.ColumnDefinition;
import com.cosyan.db.lang.expr.TableDefinition.TableWithOwnerDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.MetaRepoExecutor;
import com.cosyan.db.meta.TableProvider.TableWithOwner;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatementColumns {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddColumn extends AlterStatement {
    private final TableWithOwnerDefinition table;
    private final ColumnDefinition column;

    private TableWithOwner tableWithOwner;
    private BasicColumn basicColumn;

    @Override
    public MetaResources executeMeta(MetaRepo metaRepo, AuthToken authToken) throws ModelException {
      tableWithOwner = table.resolve(authToken);
      MaterializedTable tableMeta = metaRepo.table(tableWithOwner);
      basicColumn = tableMeta.createColumn(column);
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public Result executeData(MetaRepoExecutor metaRepo, Resources resources) throws RuleException, IOException {
      MaterializedTable tableMeta = resources.meta(tableWithOwner.resourceId());
      tableMeta.addColumn(basicColumn);
      metaRepo.syncMeta(tableMeta);
      return Result.META_OK;
    }

    @Override
    public void cancel() {
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableDropColumn extends AlterStatement {
    private final TableWithOwnerDefinition table;
    private final Ident column;

    private BasicColumn basicColumn;

    @Override
    public MetaResources executeMeta(MetaRepo metaRepo, AuthToken authToken) throws ModelException {
      MaterializedTable tableMeta = metaRepo.table(table.resolve(authToken));
      basicColumn = tableMeta.column(column);
      tableMeta.checkDeleteColumn(column);
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public Result executeData(MetaRepoExecutor metaRepo, Resources resources) throws RuleException, IOException {
      basicColumn.setDeleted(true);
      return Result.META_OK;
    }

    @Override
    public void cancel() {
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAlterColumn extends AlterStatement {
    private final TableWithOwnerDefinition table;
    private final ColumnDefinition column;

    private BasicColumn originalColumn;

    @Override
    public MetaResources executeMeta(MetaRepo metaRepo, AuthToken authToken) throws ModelException {
      MaterializedTable tableMeta = metaRepo.table(table.resolve(authToken));
      originalColumn = tableMeta.column(column.getName());
      if (originalColumn.getType() != column.getType()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', type has to remain the same.", column.getName()),
            column.getName());
      }
      if (originalColumn.isUnique() != column.isUnique()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', uniqueness has to remain the same.", column.getName()),
            column.getName());

      }
      if (originalColumn.isNullable() && !column.isNullable()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', column has to remain nullable.", column.getName()),
            column.getName());
      }
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public Result executeData(MetaRepoExecutor metaRepo, Resources resources) throws RuleException, IOException {
      originalColumn.setNullable(column.isNullable());
      originalColumn.setImmutable(column.isImmutable());
      return Result.META_OK;
    }

    @Override
    public void cancel() {
    }
  }
}
