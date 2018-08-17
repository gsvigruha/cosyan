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
package com.cosyan.db.entity;

import com.cosyan.db.conf.Config;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.session.Session;
import com.cosyan.db.transaction.TransactionHandler;
import com.google.common.collect.ImmutableList;

import lombok.val;

public class EntityHandler {

  private final TransactionHandler transactionHandler;
  private final Config config;

  public EntityHandler(Config config, TransactionHandler transactionHandler) {
    this.config = config;
    this.transactionHandler = transactionHandler;
  }

  public Result entityMeta(Session session) {
    try {
      val stmt = new EntityMetaStatement();
      val transaction = transactionHandler.begin(stmt, config);
      return session.execute(transaction);
    } catch (ConfigException e) {
      return new ErrorResult(e);
    }
  }

  public Result loadEntity(String table, String id, Session session) {
    try {
      val stmt = new LoadEntityStatement(table, id);
      val transaction = transactionHandler.begin(ImmutableList.of(stmt), config);
      return session.execute(transaction);
    } catch (ConfigException e) {
      return new ErrorResult(e);
    }
  }
}
