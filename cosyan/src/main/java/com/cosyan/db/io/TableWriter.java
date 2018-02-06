package com.cosyan.db.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.RecordReader.Record;
import com.cosyan.db.io.SeekableInputStream.SeekableSequenceInputStream;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Dependencies.ColumnReverseRuleDependencies;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableMultiIndex;
import com.cosyan.db.transaction.Resources;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

public class TableWriter extends SeekableTableReader implements TableIO {

  private final RandomAccessFile file;
  private final RecordReader reader;
  private final ImmutableList<BasicColumn> columns;
  private final ImmutableMap<String, TableIndex> uniqueIndexes;
  private final ImmutableMap<String, TableMultiIndex> multiIndexes;
  private final ImmutableMultimap<String, IndexReader> foreignIndexes;
  private final ImmutableMultimap<String, IndexReader> reversedForeignIndexes;
  private final ImmutableMap<String, BooleanRule> rules;
  private final ColumnReverseRuleDependencies reverseRules;
  private final Optional<PrimaryKey> primaryKey;

  private final long fileIndex0;
  private long actFileIndex;
  private final Set<Long> recordsToDelete = new LinkedHashSet<>();
  private final TreeMap<Long, byte[]> recordsToInsert = new TreeMap<>();

  public TableWriter(
      MaterializedTableMeta tableMeta,
      String fileName,
      ImmutableList<BasicColumn> columns,
      ImmutableMap<String, TableIndex> uniqueIndexes,
      ImmutableMap<String, TableMultiIndex> multiIndexes,
      ImmutableMultimap<String, IndexReader> foreignIndexes,
      ImmutableMultimap<String, IndexReader> reversedForeignIndexes,
      ImmutableMap<String, BooleanRule> rules,
      ColumnReverseRuleDependencies reverseRules,
      Optional<PrimaryKey> primaryKey) throws IOException {
    super(tableMeta);
    this.file = new RandomAccessFile(fileName, "rw");
    this.reader = new RecordReader(columns, new SeekableSequenceInputStream(
        new RAFBufferedInputStream(file),
        new TreeMapInputStream(recordsToInsert)),
        recordsToDelete);
    this.columns = columns;
    this.uniqueIndexes = uniqueIndexes;
    this.multiIndexes = multiIndexes;
    this.foreignIndexes = foreignIndexes;
    this.reversedForeignIndexes = reversedForeignIndexes;
    this.rules = rules;
    this.reverseRules = reverseRules;
    this.primaryKey = primaryKey;
    this.fileIndex0 = file.length();
    this.actFileIndex = fileIndex0;
  }

  public void insert(Resources resources, Object[] values, boolean checkReferencingRules)
      throws IOException, RuleException {
    long fileIndex = actFileIndex;
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      BasicColumn column = columns.get(i);
      if (!column.isNullable() && value == DataTypes.NULL) {
        throw new RuleException("Column is not nullable (mandatory).");
      }
      if (column.isUnique() && value != DataTypes.NULL) {
        try {
          uniqueIndexes.get(column.getName()).put(value, fileIndex);
        } catch (IndexException e) {
          throw new RuleException(e);
        }
      }
      if (multiIndexes.containsKey(column.getName())) {
        try {
          multiIndexes.get(column.getName()).put(value, fileIndex);
        } catch (IndexException e) {
          throw new RuleException(e);
        }
      }
      if (foreignIndexes.containsKey(column.getName())) {
        for (IndexReader foreignIndex : foreignIndexes.get(column.getName())) {
          if (!foreignIndex.contains(value)) {
            throw new RuleException(String.format(
                "Foreign key violation, value '%s' not present.", value));
          }
        }
      }
    }
    byte[] data = Serializer.serialize(values, columns);
    recordsToInsert.put(actFileIndex, data);
    actFileIndex += data.length;
    for (Map.Entry<String, BooleanRule> rule : rules.entrySet()) {
      if (!rule.getValue().check(resources, fileIndex)) {
        throw new RuleException("Constraint check " + rule.getKey() + " failed.");
      }
    }
    if (checkReferencingRules) {
      RuleDependencyReader ruleDependencyReader = new RuleDependencyReader(resources, reverseRules);
      ruleDependencyReader.checkReferencingRules(fileIndex);
    }
  }

  public void commit() throws IOException {
    try {
      file.seek(fileIndex0);
      for (byte[] data : recordsToInsert.values()) {
        file.write(data);
      }
      for (Long pos : recordsToDelete) {
        file.seek(pos);
        file.writeByte(0);
      }
    } catch (IOException e) {
      rollback();
      file.getChannel().truncate(fileIndex0);
      throw e;
    }
    for (TableIndex index : uniqueIndexes.values()) {
      try {
        index.commit();
      } catch (IOException e) {
        index.invalidate();
      }
    }
    for (TableMultiIndex index : multiIndexes.values()) {
      try {
        index.commit();
      } catch (IOException e) {
        index.invalidate();
      }
    }
  }

  public void rollback() {
    recordsToDelete.clear();
    recordsToInsert.clear();
    for (TableIndex index : uniqueIndexes.values()) {
      index.rollback();
    }
    for (TableMultiIndex index : multiIndexes.values()) {
      index.rollback();
    }
  }

  public void close() throws IOException {
    file.close();
  }

  private void delete(Record record, Predicate<Integer> checkReversedForeignIndex) throws IOException, RuleException {
    recordsToDelete.add(record.getFilePointer());
    for (BasicColumn column : columns) {
      Object value = record.getValues()[column.getIndex()];
      if (uniqueIndexes.containsKey(column.getName())) {
        uniqueIndexes.get(column.getName()).delete(value);
      }
      if (multiIndexes.containsKey(column.getName())) {
        multiIndexes.get(column.getName()).delete(value);
      }
      if (checkReversedForeignIndex.test(column.getIndex()) && reversedForeignIndexes.containsKey(column.getName())) {
        for (IndexReader reverseForeignIndex : reversedForeignIndexes.get(column.getName())) {
          if (reverseForeignIndex.contains(value)) {
            throw new RuleException(String.format(
                "Foreign key violation, key value '%s' has references.", value));
          }
        }
      }
    }
  }

  public long delete(Resources resources, ColumnMeta whereColumn) throws IOException, RuleException {
    long deletedLines = 0L;
    RecordReader reader = recordReader();
    do {
      Record record = reader.read();
      if (record == RecordReader.EMPTY) {
        reader.close();
        return deletedLines;
      }
      if (!recordsToDelete.contains(record.getFilePointer())
          && (boolean) whereColumn.value(record.getValues(), resources)) {
        delete(record, Predicates.alwaysTrue());
        deletedLines++;
      }
    } while (true);
  }

  private ImmutableList<Object[]> deleteAndCollectUpdated(
      Resources resources,
      ImmutableMap<Integer, ColumnMeta> updateExprs,
      ColumnMeta whereColumn) throws IOException, RuleException {
    ImmutableList.Builder<Object[]> updatedRecords = ImmutableList.builder();
    RecordReader reader = recordReader();
    do {
      Record record = reader.read();
      if (record == RecordReader.EMPTY) {
        reader.close();
        return updatedRecords.build();
      }
      Object[] values = record.getValues();
      if (!recordsToDelete.contains(record.getFilePointer()) && (boolean) whereColumn.value(values, resources)) {
        delete(record, (columnIndex) -> updateExprs.containsKey(columnIndex));
        Object[] newValues = new Object[values.length];
        System.arraycopy(values, 0, newValues, 0, values.length);
        for (Map.Entry<Integer, ColumnMeta> updateExpr : updateExprs.entrySet()) {
          newValues[updateExpr.getKey()] = updateExpr.getValue().value(values, resources);
        }
        updatedRecords.add(newValues);
      }
    } while (true);
  }

  public long update(Resources resources, ImmutableMap<Integer, ColumnMeta> columnExprs, ColumnMeta whereColumn)
      throws IOException, RuleException {
    ImmutableList<Object[]> valuess = deleteAndCollectUpdated(resources, columnExprs, whereColumn);
    for (Object[] values : valuess) {
      insert(resources, values, /* checkReferencingRules= */true);
    }
    return valuess.size();
  }

  public TableIndex getPrimaryKeyIndex() {
    return uniqueIndexes.get(primaryKey.get().getColumn().getName());
  }

  @Override
  public IndexReader getIndex(String name) {
    if (uniqueIndexes.containsKey(name)) {
      return uniqueIndexes.get(name);
    } else {
      return multiIndexes.get(name);
    }
  }

  @Override
  public Record get(long position) throws IOException {
    reader.seek(position);
    return reader.read();
  }

  private RecordReader recordReader() throws IOException {
    SeekableSequenceInputStream rafReader = new SeekableSequenceInputStream(
        new RAFBufferedInputStream(file),
        new TreeMapInputStream(recordsToInsert));
    return new RecordReader(columns, rafReader, recordsToDelete);
  }

  @Override
  public IterableTableReader iterableReader(Resources resources) throws IOException {
    RecordReader reader = recordReader();
    return new IterableTableReader() {

      @Override
      public Object[] next() throws IOException {
        return reader.read().getValues();
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }
    };
  }
}
