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
import com.cosyan.db.lang.expr.Statements.GlobalStatement;
import com.cosyan.db.lang.expr.TableDefinition.AggRefDefinition;
import com.cosyan.db.lang.expr.TableDefinition.FlatRefDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepoExecutor;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.References.AggRefTableMeta;
import com.cosyan.db.model.References.FlatRefTableMeta;
import com.cosyan.db.model.TableRef;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.base.Joiner;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatementRefs {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddAggRef extends AlterStatement {
    private final Ident table;
    private final AggRefDefinition ref;

    private AggRefTableMeta refTableMeta;

    @Override
    public MetaResources executeMeta(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException {
      MaterializedTable tableMeta = metaRepo.table(table);
      refTableMeta = tableMeta.createAggRef(ref);
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public Result executeData(MetaRepoExecutor metaRepo, Resources resources) {
      MaterializedTable tableMeta = resources.meta(table.getString());
      tableMeta.addRef(new TableRef(
          ref.getName().getString(),
          ref.getSelect().print(),
          metaRepo.maxRefIndex() + 1,
          /* aggr = */ true,
          refTableMeta));
      return Result.META_OK;
    }

    @Override
    public void cancel() {
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableDropAggRef extends GlobalStatement {
    private final Ident table;
    private final Ident ref;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException, IOException {
      MaterializedTable tableMeta = metaRepo.table(table);
      if (tableMeta.hasRef(ref.getString())) {
        tableMeta.dropRef(ref);
      } else {
        throw new ModelException(String.format("Aggref '%s' not found in table '%s'.", ref, table), ref);
      }
      return Result.META_OK;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddFlatRef extends AlterStatement {
    private final Ident table;
    private final FlatRefDefinition ref;

    private FlatRefTableMeta refTableMeta;

    @Override
    public MetaResources executeMeta(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException {
      MaterializedTable tableMeta = metaRepo.table(table);
      refTableMeta = tableMeta.createFlatRef(ref);
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public Result executeData(MetaRepoExecutor metaRepo, Resources resources) {
      MaterializedTable tableMeta = resources.meta(table.getString());
      tableMeta.addRef(new TableRef(
          ref.getName().getString(),
          Joiner.on(", ").join(ref.getExprs().stream().map(c -> c.print()).iterator()),
          metaRepo.maxRefIndex() + 1,
          /* aggr = */ false,
          refTableMeta));
      return Result.META_OK;
    }

    @Override
    public void cancel() {
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableDropFlatRef extends GlobalStatement {
    private final Ident table;
    private final Ident ref;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException, IOException {
      MaterializedTable tableMeta = metaRepo.table(table);
      if (tableMeta.hasRef(ref.getString())) {
        tableMeta.dropRef(ref);
      } else {
        throw new ModelException(String.format("Flatref '%s' not found in table '%s'.", ref, table), ref);
      }
      return Result.META_OK;
    }
  }
}
