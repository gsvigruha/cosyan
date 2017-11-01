package com.cosyan.db.io;

import java.io.IOException;
import java.util.HashMap;

import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

public class DependencyReader {

  private final Resources resources;

  public DependencyReader(Resources resources) {
    this.resources = resources;
  }

  public void readReferencedValues(
      Object[] sourceValues, HashMap<String, Object[]> referencedValues, ImmutableList<ForeignKey> foreignKeyChain)
      throws IOException {
    Object[] parentTableValues = sourceValues;
    String prefix = "";
    for (ForeignKey foreignKey : foreignKeyChain) {
      prefix = prefix.isEmpty() ? foreignKey.getName() : prefix + "." + foreignKey.getName();
      Object[] newSourceValues = referencedValues.get(prefix);
      if (newSourceValues == null) {
        Object foreignKeyValue = parentTableValues[foreignKey.getColumn().getIndex()];
        Ident table = new Ident(foreignKey.getRefTable().tableName());
        SeekableTableReader reader = resources.createReader(table);
        TableIndex index = resources.getPrimaryKeyIndex(table);
        long foreignKeyFilePointer = index.get0(foreignKeyValue);
        reader.seek(foreignKeyFilePointer);
        newSourceValues = reader.read().toArray();
        referencedValues.put(prefix, newSourceValues);
        reader.close();
      }
      parentTableValues = newSourceValues;
    }
  }
}
