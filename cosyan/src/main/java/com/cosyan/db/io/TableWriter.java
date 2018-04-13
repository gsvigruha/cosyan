package com.cosyan.db.io;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.RecordProvider.Record;
import com.cosyan.db.io.RecordProvider.RecordReader;
import com.cosyan.db.io.RecordProvider.SeekableRecordReader;
import com.cosyan.db.io.SeekableInputStream.SeekableSequenceInputStream;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableReader.MultiFilteredTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Dependencies.ReverseRuleDependencies;
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

  private final String fileName;
  private final SeekableOutputStream writer;
  private final MaterializedTableMeta tableMeta;
  private final SeekableRecordReader reader;
  private final ImmutableList<BasicColumn> columns;
  private final ImmutableMap<String, TableIndex> uniqueIndexes;
  private final ImmutableMap<String, TableMultiIndex> multiIndexes;
  private final ImmutableMultimap<String, IndexReader> foreignIndexes;
  private final ImmutableMultimap<String, IndexReader> reversedForeignIndexes;
  private final ImmutableMap<String, BooleanRule> rules;
  private final ReverseRuleDependencies reverseRules;
  private final Optional<PrimaryKey> primaryKey;

  private final long fileIndex0;
  private long actFileIndex;
  private final Set<Long> recordsToDelete = new LinkedHashSet<>();
  private final TreeMap<Long, byte[]> recordsToInsert = new TreeMap<>();

  public TableWriter(
      MaterializedTableMeta tableMeta,
      String fileName,
      SeekableOutputStream fileWriter,
      SeekableInputStream fileReader,
      ImmutableList<BasicColumn> columns,
      ImmutableMap<String, TableIndex> uniqueIndexes,
      ImmutableMap<String, TableMultiIndex> multiIndexes,
      ImmutableMultimap<String, IndexReader> foreignIndexes,
      ImmutableMultimap<String, IndexReader> reversedForeignIndexes,
      ImmutableMap<String, BooleanRule> rules,
      ReverseRuleDependencies reverseRules,
      Optional<PrimaryKey> primaryKey) throws IOException {
    super(tableMeta);
    this.tableMeta = tableMeta;
    this.fileName = fileName;
    this.writer = fileWriter;
    this.reader = new SeekableRecordReader(columns, new SeekableSequenceInputStream(
        fileReader,
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
    this.fileIndex0 = fileReader.length();
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
      if (value != DataTypes.NULL && multiIndexes.containsKey(column.getName())) {
        try {
          multiIndexes.get(column.getName()).put(value, fileIndex);
        } catch (IndexException e) {
          throw new RuleException(e);
        }
      }
      if (value != DataTypes.NULL && foreignIndexes.containsKey(column.getName())) {
        for (IndexReader foreignIndex : foreignIndexes.get(column.getName())) {
          if (!foreignIndex.contains(value)) {
            throw new RuleException(String.format(
                "Foreign key violation, value '%s' not present.", value));
          }
        }
      }
    }
    byte[] data = Serializer.serialize(values, columns);
    recordsToInsert.put(fileIndex, data);
    actFileIndex += data.length;
    for (Map.Entry<String, BooleanRule> rule : rules.entrySet()) {
      if (!rule.getValue().check(resources, fileIndex)) {
        throw new RuleException(
            "Constraint check " + rule.getKey() + " failed.");
      }
    }
    if (checkReferencingRules) {
      RuleDependencyReader ruleDependencyReader = new RuleDependencyReader(resources, reverseRules);
      ruleDependencyReader.checkReferencingRules(new Record(fileIndex, values));
    }
  }

  public void commit() throws IOException {
    try {
      if (recordsToInsert.size() > 1) {
        ByteArrayOutputStream finalBuffer = new ByteArrayOutputStream(1024);
        for (byte[] data : recordsToInsert.values()) {
          finalBuffer.write(data);
        }
        writer.write(fileIndex0, finalBuffer.toByteArray());
      } else {
        for (byte[] data : recordsToInsert.values()) {
          writer.write(fileIndex0, data);
        }
      }
      for (Long pos : recordsToDelete) {
        writer.write(pos, new byte[] { 0 });
      }
    } catch (IOException e) {
      rollback();
      writer.getChannel().truncate(fileIndex0);
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
    writer.close();
  }

  private void delete(Record record, Resources resources, Predicate<Integer> checkReversedForeignIndex,
      boolean checkReverseRuleDependencies)
      throws IOException, RuleException {
    recordsToDelete.add(record.getFilePointer());
    for (BasicColumn column : columns) {
      Object value = record.getValues()[column.getIndex()];
      if (uniqueIndexes.containsKey(column.getName())) {
        uniqueIndexes.get(column.getName()).delete(value);
      }
      if (multiIndexes.containsKey(column.getName())) {
        multiIndexes.get(column.getName()).delete(value, record.getFilePointer());
      }
      if (checkReversedForeignIndex.test(column.getIndex())) {
        if (reversedForeignIndexes.containsKey(column.getName())) {
          for (IndexReader reverseForeignIndex : reversedForeignIndexes.get(column.getName())) {
            if (reverseForeignIndex.contains(value)) {
              throw new RuleException(String.format(
                  "Foreign key violation, key value '%s' has references.", value));
            }
          }
        }
      }
    }
    if (checkReverseRuleDependencies) {
      RuleDependencyReader ruleDependencyReader = new RuleDependencyReader(resources, reverseRules);
      ruleDependencyReader.checkReferencingRules(record);
    }
  }

  private long delete(RecordProvider recordProvider, Resources resources, ColumnMeta whereColumn)
      throws IOException, RuleException {
    long deletedLines = 0L;
    do {
      Record record = recordProvider.read();
      if (record == RecordReader.EMPTY) {
        recordProvider.close();
        return deletedLines;
      }
      if (!recordsToDelete.contains(record.getFilePointer())
          && (boolean) whereColumn.value(record.getValues(), resources)) {
        delete(record, resources, Predicates.alwaysTrue(), /* checkReverseRuleDependencies= */true);
        deletedLines++;
      }
    } while (true);
  }

  public long delete(Resources resources, ColumnMeta whereColumn) throws IOException, RuleException {
    RecordReader reader = recordReader();
    try {
      return delete(reader, resources, whereColumn);
    } finally {
      reader.close();
    }
  }

  public long deleteWithIndex(Resources resources, ColumnMeta whereColumn, VariableEquals clause)
      throws IOException, RuleException {
    MultiFilteredTableReader reader = indexFilteredReader(resources, whereColumn, clause);
    return delete(reader, resources, whereColumn);
  }

  private ImmutableList<Object[]> deleteAndCollectUpdated(
      RecordProvider recordProvider,
      Resources resources,
      ImmutableMap<Integer, ColumnMeta> updateExprs,
      ColumnMeta whereColumn) throws IOException, RuleException {
    ImmutableList.Builder<Object[]> updatedRecords = ImmutableList.builder();
    do {
      Record record = recordProvider.read();
      if (record == RecordReader.EMPTY) {
        recordProvider.close();
        return updatedRecords.build();
      }
      Object[] values = record.getValues();
      if (!recordsToDelete.contains(record.getFilePointer()) && (boolean) whereColumn.value(values, resources)) {
        delete(record, resources, (columnIndex) -> updateExprs.containsKey(columnIndex), /*
                                                                                          * checkReverseRuleDependencies=
                                                                                          */false);
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
    RecordReader reader = recordReader();
    try {
      ImmutableList<Object[]> valuess = deleteAndCollectUpdated(reader, resources, columnExprs, whereColumn);
      for (Object[] values : valuess) {
        insert(resources, values, /* checkReferencingRules= */true);
      }
      return valuess.size();
    } finally {
      reader.close();
    }
  }

  public long updateWithIndex(
      Resources resources,
      ImmutableMap<Integer, ColumnMeta> columnExprs,
      ColumnMeta whereColumn,
      VariableEquals clause) throws IOException, RuleException {
    MultiFilteredTableReader reader = indexFilteredReader(resources, whereColumn, clause);
    ImmutableList<Object[]> valuess = deleteAndCollectUpdated(reader, resources, columnExprs, whereColumn);
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

  @Override
  public Record get(Object key, Resources resources) throws IOException {
    IndexReader index = resources.getPrimaryKeyIndex(tableMeta.tableName());
    long filePointer = index.get(key)[0];
    return get(filePointer);
  }

  private RecordReader recordReader() throws IOException {
    @SuppressWarnings("resource") // RecordReader closes SequenceInputStream.
    InputStream rafReader = new SequenceInputStream(
        new BufferedInputStream(new FileInputStream(fileName)),
        new TreeMapInputStream(recordsToInsert));
    return new RecordReader(columns, rafReader, recordsToDelete);
  }

  private MultiFilteredTableReader indexFilteredReader(Resources resources, ColumnMeta whereColumn,
      VariableEquals clause) {
    return new MultiFilteredTableReader(this, whereColumn, resources) {
      @Override
      protected void readPositions() throws IOException {
        IndexReader index = resources.getIndex(tableMeta.tableName(), clause.getIdent().getString());
        positions = index.get(clause.getValue());
      }
    };
  }

  @Override
  public IterableTableReader iterableReader() throws IOException {
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
