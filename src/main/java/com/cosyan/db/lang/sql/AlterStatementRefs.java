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
import com.cosyan.db.lang.expr.TableDefinition.TableWithOwnerDefinition;
import com.cosyan.db.lang.expr.TableDefinition.ViewDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaWriter;
import com.cosyan.db.meta.TableProvider.TableWithOwner;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableRef;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatementRefs {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddView extends AlterStatement {
    private final TableWithOwnerDefinition table;
    private final ViewDefinition ref;

    private TableWithOwner tableWithOwner;
    private TableMeta refTableMeta;

    @Override
    public MetaResources executeMeta(MetaWriter metaRepo, AuthToken authToken) throws ModelException, GrantException, IOException {
      tableWithOwner = table.resolve(authToken);
      MaterializedTable tableMeta = metaRepo.table(tableWithOwner, authToken);
      refTableMeta = tableMeta.createView(ref, tableMeta.owner());
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public Result executeData(MetaWriter metaRepo, Resources resources) {
      MaterializedTable tableMeta = resources.meta(tableWithOwner.resourceId());
      tableMeta.addRef(new TableRef(
          ref.getName().getString(),
          ref.getSelect().print(),
          metaRepo.maxRefIndex() + 1,
          refTableMeta));
      return Result.META_OK;
    }

    @Override
    public void cancel() {
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableDropView extends GlobalStatement {
    private final TableWithOwnerDefinition table;
    private final Ident ref;

    @Override
    public Result execute(MetaWriter metaRepo, AuthToken authToken) throws ModelException, GrantException, IOException {
      MaterializedTable tableMeta = metaRepo.table(table.resolve(authToken), authToken);
      if (tableMeta.hasRef(ref.getString())) {
        tableMeta.dropRef(ref);
      } else {
        throw new ModelException(String.format("Aggref '%s' not found in table '%s'.", ref, table), ref);
      }
      return Result.META_OK;
    }
  }
}
