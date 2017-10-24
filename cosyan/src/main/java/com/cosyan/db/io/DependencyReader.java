package com.cosyan.db.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.SourceValues;
import com.cosyan.db.model.TableIndex;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class DependencyReader {

  public static DependencyReader NO_DEPS = new DependencyReader(ImmutableList.of());

  @Data
  public static class DependentTableReader {
    private final ForeignKey foreignKey;
    private final TableIndex index;
    private final SeekableTableReader tableReader;
    private final List<DependentTableReader> deps;
  }

  private final List<DependentTableReader> deps;

  public DependencyReader(List<DependentTableReader> deps) {
    this.deps = deps;
  }

  public Map<String, Object[]> readReferencedValues(Object[] sourceValues) throws IOException {
    Map<String, Object[]> referencedValues = new HashMap<>();
    readReferencedValues(referencedValues, "", sourceValues, deps);
    return referencedValues;
  }

  private void readReferencedValues(
      Map<String, Object[]> referencedValues,
      String prefix,
      Object[] parentTableValues,
      List<DependentTableReader> dependencies) throws IOException {
    for (DependentTableReader reader : dependencies) {
      Object foreignKeyValue = parentTableValues[reader.foreignKey.getColumn().getIndex()];
      long foreignKeyFilePointer = reader.index.get0(foreignKeyValue);
      reader.tableReader.seek(foreignKeyFilePointer);
      SourceValues sourceValues = reader.tableReader.read();
      String tableReferenceName = prefix + reader.foreignKey.getName();
      referencedValues.put(tableReferenceName, sourceValues.toArray());
      readReferencedValues(referencedValues, tableReferenceName + ".", sourceValues.toArray(), reader.deps);
    }
  }
}
