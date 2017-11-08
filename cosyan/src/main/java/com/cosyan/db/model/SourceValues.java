package com.cosyan.db.model;

import java.io.IOException;
import java.util.HashMap;

import com.cosyan.db.io.DependencyReader;
import com.cosyan.db.model.References.SimpleReferencingColumn;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

public class SourceValues {

  protected final Object[] sourceValues;

  public SourceValues(Object[] sourceValues) {
    this.sourceValues = sourceValues;
  }

  public Object sourceValue(int index) {
    return sourceValues[index];
  }

  public Object refTableValue(SimpleReferencingColumn column) throws IOException {
    throw new UnsupportedOperationException();
  }

  public static SourceValues of(Object[] sourceValues) {
    return new SourceValues(sourceValues);
  }

  public boolean isEmpty() {
    return false;
  }

  public static class ReferencingSourceValues extends SourceValues {

    private final DependencyReader reader;
    private final HashMap<String, Object[]> referencedValues;

    public ReferencingSourceValues(Resources resources, Object[] sourceValues,
        HashMap<String, Object[]> referencedValues) {
      super(sourceValues);
      this.reader = new DependencyReader(resources);
      this.referencedValues = referencedValues;
    }

    @Override
    public Object refTableValue(SimpleReferencingColumn column) throws IOException {
      Object[] values = referencedValues.get(column.tableNameWithChain());
      if (values == null) {
        reader.readReferencedValues(sourceValues, referencedValues, column);
        values = referencedValues.get(column.tableNameWithChain());
      }
      assert values != null : String.format("No values for '%s'.", column.tableNameWithChain());
      return values[column.getIndex()];
    }

    public static ReferencingSourceValues of(Resources resources, Object[] sourceValues) {
      return new ReferencingSourceValues(resources, sourceValues, new HashMap<>());
    }

    public static ReferencingSourceValues of(
        Resources resources, Object[] sourceValues, HashMap<String, Object[]> initialReferencedValues) {
      return new ReferencingSourceValues(resources, sourceValues, initialReferencedValues);
    }
  }

  private static class EmptySourceValues extends SourceValues {

    public EmptySourceValues() {
      super(new Object[] {});
    }

    @Override
    public boolean isEmpty() {
      return true;
    }
  }

  public static final SourceValues EMPTY = new EmptySourceValues();

  public ImmutableList<Object> toList() {
    return ImmutableList.copyOf(sourceValues);
  }

  public Object[] toArray() {
    return sourceValues;
  }
}
