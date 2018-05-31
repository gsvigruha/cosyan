package com.cosyan.db.entity;

import java.io.IOException;
import java.util.Optional;

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
import com.cosyan.db.model.TableUniqueIndex;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class LoadEntityStatement extends Statement {

  private final String table;
  private final String id;

  private ImmutableList<BasicColumn> header;
  private BasicColumn column;

  @Override
  public MetaResources compile(MetaRepo metaRepo) throws ModelException {
    MaterializedTable tableMeta = metaRepo.table(new Ident(table, new Loc(0, 0)));
    header = tableMeta.columns().values().asList();
    Optional<BasicColumn> pkColumn = tableMeta.pkColumn();
    if (pkColumn.isPresent()) {
      column = pkColumn.get();
    } else {
      throw new ModelException(String.format("Table '%s' does not have a primary key.", table), new Loc(0, 0));
    }
    return tableMeta.readResources();
  }

  @Override
  public Entity execute(Resources resources) throws RuleException, IOException {
    Object key = column.getType().fromString(id);
    SeekableTableReader reader = resources.reader(table);
    TableUniqueIndex index = resources.getPrimaryKeyIndex(table);
    Record record = reader.get(index.get0(key));
    return new Entity(table, column.getName(), header, record.getValues());
  }

  @Override
  public void cancel() {
    // TODO Auto-generated method stub

  }
}
