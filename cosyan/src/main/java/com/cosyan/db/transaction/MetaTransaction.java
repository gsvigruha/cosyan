package com.cosyan.db.transaction;

public abstract class MetaTransaction extends Transaction {

  public MetaTransaction(long trxNumber) {
    super(trxNumber);
  }
}
