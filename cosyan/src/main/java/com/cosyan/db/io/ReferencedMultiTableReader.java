package com.cosyan.db.io;

import java.io.IOException;

import com.cosyan.db.io.AggrReader.GlobalAggrTableReader;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.SourceValues;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

public class ReferencedMultiTableReader extends TableReader {

  private final MultiFilteredTableReader filterReader;
  private final GlobalAggrTableReader aggrReader;

  private long[] positions;

  public ReferencedMultiTableReader(MaterializedTableMeta tableMeta, Resources resources,
      ImmutableList<AggrColumn> columns) throws IOException {
    super();
    this.filterReader = new MultiFilteredTableReader(resources.createReader(tableMeta.tableName()),
        ColumnMeta.TRUE_COLUMN) {

      @Override
      protected void readPositions() throws IOException {
        this.positions = ReferencedMultiTableReader.this.positions;
      }
    };
    this.aggrReader = new GlobalAggrTableReader(this.filterReader, columns, ColumnMeta.TRUE_COLUMN);
  }

  @Override
  public void close() throws IOException {
    aggrReader.close();
  }

  @Override
  public SourceValues read() throws IOException {
    return aggrReader.read();
  }

  public void seek(long[] foreignKeyFilePointers) {
    this.positions = foreignKeyFilePointers;
  }
}
