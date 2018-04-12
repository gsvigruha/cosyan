package com.cosyan.db.model.stat;

public class TableStats {
  private long cnt;
  private long lastID;

  public boolean isEmpty() {
    return cnt == 0;
  }

  public void insert(int insertedLines) {
    cnt += insertedLines;
    lastID += insertedLines;
  }

  public void delete(long deletedLines) {
    cnt -= deletedLines;
  }

  public long lastID() {
    return lastID;
  }
}
