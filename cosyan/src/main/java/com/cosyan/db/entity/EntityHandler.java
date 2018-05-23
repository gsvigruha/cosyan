package com.cosyan.db.entity;

import com.cosyan.db.entity.EntitySearchStatement.SearchField;
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

  public Result entityMeta(Session session) {
    val stmt = new EntityMetaStatement();
    val transaction = transactionHandler.begin(stmt);
    return transaction.execute(metaRepo, session);
  }

  public Result loadEntity(String table, String id, Session session) {
    val stmt = new LoadEntityStatement(table, id);
    val transaction = transactionHandler.begin(ImmutableList.of(stmt));
    return transaction.execute(metaRepo, session);
  }

  public Result loadEntityList(String table, String column, String id, Session session) {
    val stmt = new LoadEntityListStatement(table, column, id);
    val transaction = transactionHandler.begin(ImmutableList.of(stmt));
    return transaction.execute(metaRepo, session);
  }

  public Result deleteEntity(String table, String id, Session session) {
    val stmt = new DeleteEntityStatement(table, id);
    val transaction = transactionHandler.begin(ImmutableList.of(stmt));
    return transaction.execute(metaRepo, session);
  }

  public Result searchEntity(String table, ImmutableList<SearchField> searchFields, Session session) {
    val stmt = new EntitySearchStatement(table, searchFields);
    val transaction = transactionHandler.begin(ImmutableList.of(stmt));
    return transaction.execute(metaRepo, session);
  }
}
