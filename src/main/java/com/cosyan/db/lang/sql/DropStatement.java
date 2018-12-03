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
import com.cosyan.db.lang.expr.Statements.GlobalStatement;
import com.cosyan.db.lang.expr.TableDefinition.TableColumnDefinition;
import com.cosyan.db.lang.expr.TableDefinition.TableWithOwnerDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaWriter;
import com.cosyan.db.meta.TableProvider.TableWithOwner;
import com.cosyan.db.meta.View.TopLevelView;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DropStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DropTable extends GlobalStatement {
    private final TableWithOwnerDefinition table;

    private TableWithOwner tableWithOwner;

    @Override
    public Result execute(MetaWriter metaRepo, AuthToken authToken) throws ModelException, IOException, GrantException {
      tableWithOwner = table.resolve(authToken);
      MaterializedTable tableMeta = metaRepo.table(tableWithOwner, authToken);
      if (!tableMeta.foreignKeys().isEmpty()) {
        ForeignKey foreignKey = tableMeta.foreignKeys().values().iterator().next();
        throw new ModelException(String.format("Cannot drop table '%s', has foreign key '%s'.",
            tableWithOwner, foreignKey),
            table.getTable());
      }
      if (!tableMeta.reverseForeignKeys().isEmpty()) {
        ReverseForeignKey foreignKey = tableMeta.reverseForeignKeys().values().iterator().next();
        throw new ModelException(String.format("Cannot drop table '%s', referenced by foreign key '%s.%s'.",
            tableWithOwner,
            foreignKey.getRefTable().fullName(),
            foreignKey.getReverse()),
            table.getTable());
      }
      metaRepo.dropTable(tableMeta, authToken);
      return Result.META_OK;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DropIndex extends GlobalStatement {
    private final TableColumnDefinition tableColumn;

    private TableWithOwner tableWithOwner;
    private BasicColumn basicColumn;

    @Override
    public Result execute(MetaWriter metaRepo, AuthToken authToken) throws ModelException, IOException, GrantException {
      tableWithOwner = tableColumn.getTable().resolve(authToken);
      MaterializedTable tableMeta = metaRepo.table(tableWithOwner, authToken);
      basicColumn = tableMeta.column(tableColumn.getColumn());
      if (basicColumn.isUnique()) {
        throw new ModelException(String.format("Cannot drop index '%s.%s', column is unique.",
            tableMeta.fullName(), basicColumn.getName()), tableColumn.getColumn());
      }
      tableMeta.dropIndex(basicColumn);
      return Result.META_OK;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DropView extends GlobalStatement {
    private final TableWithOwnerDefinition table;

    private TableWithOwner tableWithOwner;

    @Override
    public Result execute(MetaWriter metaRepo, AuthToken authToken) throws ModelException, IOException, GrantException {
      tableWithOwner = table.resolve(authToken);
      TopLevelView view = metaRepo.view(tableWithOwner, authToken);
      metaRepo.dropView(view, authToken);
      return Result.META_OK;
    }
  }
}
