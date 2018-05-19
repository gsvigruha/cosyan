package com.cosyan.db.entity;

import java.io.IOException;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.RecordProvider.Record;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class LoadEntityListStatement extends Statement {

  private final String table;
  private final String indexColumn;
  private final String id;

  private ImmutableList<BasicColumn> header;
  private BasicColumn column;

  @Override
  public MetaResources compile(MetaRepo metaRepo) throws ModelException {
    MaterializedTable tableMeta = metaRepo.table(new Ident(table));
    header = tableMeta.columns().values().asList();
    column = tableMeta.column(new Ident(indexColumn));
    if (!column.isIndexed()) {
      throw new ModelException(String.format("Column '%s.%s' is not indexed.", table, indexColumn), new Loc(0, 0));
    }
    return tableMeta.readResources();
  }

  @Override
  public EntityList execute(Resources resources) throws RuleException, IOException {
    Object key = column.getType().fromString(id);
    SeekableTableReader reader = resources.reader(table);
    IndexReader index = resources.getIndex(table, indexColumn);
    long[] pointers = index.get(key);
    ImmutableList.Builder<Object[]> valuess = ImmutableList.builder();
    for (long pointer : pointers) {
      Record record = reader.get(pointer);
      valuess.add(record.getValues());
    }
    return new EntityList(table, header, valuess.build());
  }

  @Override
  public void cancel() {
    // TODO Auto-generated method stub

  }
}
