package com.cosyan.db.model.stat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.cosyan.db.conf.Config;

public class TableStats {

  private final Config config;
  private final String tableName;

  private long cnt = 0L;

  public TableStats(Config config, String tableName) throws IOException {
    this.config = config;
    this.tableName = tableName;
  }

  public void load() throws IOException {
    File statFile = new File(config.statDir() + File.separator + tableName);
    if (statFile.exists()) {
      DataInputStream stream = new DataInputStream(new FileInputStream(statFile));
      cnt = stream.readLong();
      stream.close();
    }
  }

  public void save() throws IOException {
    File statFile = new File(config.statDir() + File.separator + tableName);
    if (statFile.exists()) {
      DataOutputStream stream = new DataOutputStream(new FileOutputStream(statFile));
      stream.writeLong(cnt);
      stream.close();
    }
  }

  public boolean isEmpty() {
    return cnt == 0;
  }

  public void insert(long insertedLines) {
    cnt += insertedLines;
  }

  public void delete(long deletedLines) {
    cnt -= deletedLines;
  }
}
