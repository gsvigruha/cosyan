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

import com.cosyan.db.io.Indexes.IndexWriter;
import com.cosyan.db.model.BasicColumn;

public interface MetaRepoExecutor {

  IndexWriter registerIndex(MaterializedTable meta, BasicColumn basicColumn) throws IOException;

  void syncMeta(MaterializedTable tableMeta);

  int maxRefIndex();

}
