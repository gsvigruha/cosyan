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
package com.cosyan.db.meta;

import java.util.Optional;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;

public interface TableProvider {

  public static class TableWithOwner {
    private final Ident table;
    private final String owner;

    private TableWithOwner(Ident table, String owner) {
      this.table = table;
      this.owner = owner;
    }

    public Ident getTable() {
      return table;
    }

    public String getOwner() {
      return owner;
    }

    public String resourceId() {
      return owner + "." + table.getString();
    }

    @Override
    public String toString() {
      return resourceId();
    }

    public static TableWithOwner create(Ident table, Optional<Ident> owner, AuthToken authToken) {
      if (owner.isPresent()) {
        return new TableWithOwner(table, owner.get().getString());
      } else {
        return new TableWithOwner(table, authToken.username());
      }
    }

    public static TableWithOwner of(Ident table, String owner) {
      return new TableWithOwner(table, owner);
    }
  }

  public ExposedTableMeta tableMeta(TableWithOwner table) throws ModelException;

  public TableProvider tableProvider(Ident ident, String owner) throws ModelException;

}
