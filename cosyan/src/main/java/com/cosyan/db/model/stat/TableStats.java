package com.cosyan.db.model.stat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.cosyan.db.conf.Config;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableReader.MaterializedTableReader;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MaterializedTableMeta;
import com.google.common.collect.ImmutableMap;

public class TableStats {

  private final Config config;
  private final String tableName;

  private long cnt = 0L;
  private long lastID = 0L;

  public TableStats(Config config, String tableName) throws IOException {
    this.config = config;
    this.tableName = tableName;
  }

  public void load(MaterializedTableMeta tableMeta) throws IOException {
    File statFile = new File(config.statDir() + File.separator + tableName);
    if (statFile.exists()) {
      DataInputStream stream = new DataInputStream(new FileInputStream(statFile));
      cnt = stream.readLong();
      lastID = stream.readLong();
      stream.close();
    } else {
      boolean hasID = tableMeta.primaryKey().map(key -> key.getColumn().getType() == DataTypes.IDType).orElse(false);
      IterableTableReader reader = new MaterializedTableReader(
          tableMeta,
          tableMeta.fileName(),
          tableMeta.fileReader(),
          tableMeta.allColumns(),
          ImmutableMap.of()).iterableReader();
      Object[] record;
      while ((record = reader.next()) != null) {
        cnt++;
        if (hasID) {
          lastID = Math.max(lastID, (Long) record[0]);
        }
      }
      reader.close();
    }
  }

  public void persist() throws IOException {
    File statFile = new File(config.statDir() + File.separator + tableName);
    if (statFile.exists()) {
      DataOutputStream stream = new DataOutputStream(new FileOutputStream(statFile));
      stream.writeLong(cnt);
      stream.writeLong(lastID);
      stream.close();
    }
  }

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
