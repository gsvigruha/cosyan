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
