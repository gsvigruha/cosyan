package com.cosyan.db.entity;

import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.session.Session;
import com.cosyan.db.transaction.TransactionHandler;
import com.google.common.collect.ImmutableList;

import lombok.val;

public class EntityHandler {

  private final TransactionHandler transactionHandler;
  private final MetaRepo metaRepo;

  public EntityHandler(MetaRepo metaRepo, TransactionHandler transactionHandler) {
    this.metaRepo = metaRepo;
    this.transactionHandler = transactionHandler;
  }

  public Result entityMeta(Session session) throws ConfigException {
    val stmt = new EntityMetaStatement();
    val transaction = transactionHandler.begin(stmt, metaRepo.config());
    return transaction.execute(metaRepo, session);
  }

  public Result loadEntity(String table, String id, Session session) throws ConfigException {
    val stmt = new LoadEntityStatement(table, id);
    val transaction = transactionHandler.begin(ImmutableList.of(stmt), metaRepo.config());
    return transaction.execute(metaRepo, session);
  }
}
