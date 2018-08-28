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

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.Statements.GlobalStatement;
import com.cosyan.db.lang.expr.TableDefinition.TableWithOwnerDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.Grants.GrantToken;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.TableProvider.TableWithOwner;
import com.cosyan.db.model.Ident;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class GrantStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Grant extends GlobalStatement {
    private final Ident user;
    private final TableWithOwnerDefinition table;
    private final String method;
    private final boolean withGrantOption;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException {
      TableWithOwner tableWithOwner = table.resolve(authToken);
      if (!tableWithOwner.getTable().getString().equals("*") && tableWithOwner.getOwner().equals("*")) {
        throw new ModelException(String.format("Wrong table '%s'. If the table name is '*', the owner also has to be '*'.", tableWithOwner), tableWithOwner.getTable());
      }
      GrantToken grantToken;
      grantToken = new GrantToken(
          user.getString(),
          Grants.Method.valueOf(method.toUpperCase()),
          tableWithOwner.getOwner(),
          tableWithOwner.getTable().getString(),
          withGrantOption);
      metaRepo.createGrant(grantToken, authToken);
      return Result.META_OK;
    }
  }
}
