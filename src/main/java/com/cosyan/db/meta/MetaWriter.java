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

import java.io.IOException;
import java.util.List;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.conf.Config;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.Grants.GrantToken;
import com.cosyan.db.meta.Grants.Method;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.TableProvider.TableWithOwner;
import com.cosyan.db.transaction.MetaResources.TableMetaResource;

public interface MetaWriter {

  void syncMeta(MaterializedTable tableMeta);

  int maxRefIndex();

  void metaRepoWriteUnlock();

  void registerTable(MaterializedTable tableMeta) throws IOException;

  void dropTable(MaterializedTable tableMeta, AuthToken authToken) throws IOException, GrantException;

  MaterializedTable table(TableWithOwner table, AuthToken authToken) throws ModelException, GrantException;

  Config config();

  boolean hasTable(String tableName, String owner);

  void createGrant(GrantToken grant, AuthToken authToken) throws GrantException;

  void createUser(String username, String password, AuthToken authToken) throws GrantException;

  void checkAccess(TableMetaResource resource, AuthToken authToken) throws GrantException;

  boolean hasAccess(MaterializedTable tableMeta, AuthToken authToken, Method method);

  List<MaterializedTable> getTables(AuthToken authToken);

  void resetAndReadTables() throws DBException;
}
