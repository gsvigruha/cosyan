package com.cosyan.db.index;

import lombok.Data;

public class IndexStat {
  @Data
  public static class ByteTrieStat {
    private final long indexFileSize;
    private final int inMemNodes;
    private final int pendingNodes;
  }

  @Data
  public static class ByteMultiTrieStat {
    private final long trieFileSize;
    private final long indexFileSize;
    private final int trieInMemNodes;
    private final int triePendingNodes;
    private final int pendingNodes;
  }
}
