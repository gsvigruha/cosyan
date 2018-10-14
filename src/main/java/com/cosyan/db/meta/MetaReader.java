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

import org.json.JSONArray;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.index.IndexStat.ByteMultiTrieStat;
import com.cosyan.db.index.IndexStat.ByteTrieStat;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.transaction.MetaResources.TableMetaResource;
import com.google.common.collect.ImmutableMap;

public interface MetaReader extends TableProvider {

  MaterializedTable table(TableWithOwner table) throws ModelException;

  void metaRepoReadUnlock();

  ImmutableMap<String, TableStat> tableStats() throws IOException;

  ImmutableMap<String, ByteTrieStat> uniqueIndexStats() throws IOException;

  ImmutableMap<String, ByteMultiTrieStat> multiIndexStats() throws IOException;

  JSONArray collectUsers();

  List<MaterializedTable> getTables(AuthToken authToken);

  void checkAccess(TableMetaResource resource, AuthToken authToken) throws GrantException;

  IndexReader getIndex(String id) throws RuleException;
}
