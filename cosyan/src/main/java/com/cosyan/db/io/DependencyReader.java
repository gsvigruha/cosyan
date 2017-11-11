package com.cosyan.db.io;

import java.io.IOException;
import java.util.HashMap;

import javax.annotation.Nullable;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.References.ReferencedSimpleTableMeta;
import com.cosyan.db.model.References.ReferencedTableMeta;
import com.cosyan.db.model.References.ReferencingColumn;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.transaction.Resources;

public class DependencyReader {

  private final Resources resources;

  public DependencyReader(Resources resources) {
    this.resources = resources;
  }

  public void readReferencedValues(
      Object[] sourceValues, HashMap<String, Object[]> referencedValues, ReferencingColumn column)
      throws IOException {
    ReferencedTableMeta tableMeta = column.getTableMeta();
    readReferencedValues(sourceValues, referencedValues, tableMeta);
  }

  private Object[] readReferencedValues(
      Object[] sourceValues,
      HashMap<String, Object[]> referencedValues,
      @Nullable ReferencedTableMeta tableMeta)
      throws IOException {
    if (tableMeta == null) {
      return sourceValues;
    }
    Object[] newSourceValues = referencedValues.get(tableMeta.tableNameWithChain());
    if (newSourceValues == null) {
      if (tableMeta instanceof ReferencedSimpleTableMeta) {
        ForeignKey foreignKey = ((ReferencedSimpleTableMeta) tableMeta).getForeignKey();
        Object[] parentTableValues = readReferencedValues(sourceValues, referencedValues, tableMeta.getParent());
        Object foreignKeyValue = parentTableValues[foreignKey.getColumn().getIndex()];
        String table = foreignKey.getRefTable().tableName();
        SeekableTableReader reader = resources.createReader(table);
        TableIndex index = resources.getPrimaryKeyIndex(table);
        long foreignKeyFilePointer = index.get0(foreignKeyValue);
        reader.seek(foreignKeyFilePointer);
        newSourceValues = reader.read().toArray();
        referencedValues.put(tableMeta.tableNameWithChain(), newSourceValues);
        reader.close();
      } else if (tableMeta instanceof ReferencedMultiTableMeta) {
        ReferencedMultiTableMeta multiTableMeta = (ReferencedMultiTableMeta) tableMeta;
        ReverseForeignKey foreignKey = multiTableMeta.getReverseForeignKey();
        Object[] parentTableValues = readReferencedValues(sourceValues, referencedValues, tableMeta.getParent());
        Object foreignKeyValue = parentTableValues[foreignKey.getColumn().getIndex()];
        IndexReader index = resources.getIndex(foreignKey);
        long[] foreignKeyFilePointers = index.get(foreignKeyValue);
        ReferencedMultiTableReader reader = multiTableMeta.reader(resources);
        reader.seek(foreignKeyFilePointers);
        newSourceValues = reader.read().toArray();
        referencedValues.put(tableMeta.tableNameWithChain(), newSourceValues);
      } else {
        throw new AssertionError();
      }
    }
    return newSourceValues;
  }
}
