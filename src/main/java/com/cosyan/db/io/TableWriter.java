/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.cosyan.db.io.Indexes.IndexWriter;
import com.cosyan.db.io.RecordProvider.Record;
import com.cosyan.db.io.RecordProvider.RecordReader;
import com.cosyan.db.io.RecordProvider.SeekableRecordReader;
import com.cosyan.db.io.SeekableInputStream.SeekableSequenceInputStream;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableReader.MultiFilteredTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.meta.Dependencies.ReverseRuleDependencies;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.TableContext;
import com.cosyan.db.model.TableMultiIndex;
import com.cosyan.db.model.TableUniqueIndex;
import com.cosyan.db.transaction.Resources;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

public class TableWriter extends SeekableTableReader implements TableIO {

  private final String fileName;
  private final SeekableOutputStream writer;
  private final MaterializedTable tableMeta;
  private final SeekableRecordReader reader;
  private final ImmutableList<BasicColumn> allColumns;
  private final ImmutableList<BasicColumn> activeColumns;
  private final ImmutableMap<String, TableUniqueIndex> uniqueIndexes;
  private final ImmutableMap<String, TableMultiIndex> multiIndexes;
  private final ImmutableMultimap<String, IndexReader> foreignIndexes;
  private final ImmutableMultimap<String, IndexReader> reversedForeignIndexes;
  private final ImmutableMap<String, BooleanRule> rules;
  private final ReverseRuleDependencies reverseRules;
  private final Optional<PrimaryKey> primaryKey;

  private long fileIndex0;
  private long actFileIndex;
  private final Set<Long> recordsToDelete = new LinkedHashSet<>();
  private final TreeMap<Long, byte[]> recordsToInsert = new TreeMap<>();

  private boolean cancelled = false;

  public TableWriter(
      MaterializedTable tableMeta,
      String fileName,
      SeekableOutputStream fileWriter,
      SeekableInputStream fileReader,
      ImmutableList<BasicColumn> allColumns,
      ImmutableMap<String, TableUniqueIndex> uniqueIndexes,
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
    this.reader = new SeekableRecordReader(allColumns, new SeekableSequenceInputStream(
        fileReader,
        new TreeMapInputStream(recordsToInsert)),
        recordsToDelete);
    this.allColumns = allColumns;
    this.activeColumns = allColumns.stream().filter(c -> !c.isDeleted()).collect(ImmutableList.toImmutableList());
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

  private static Object check(BasicColumn column, Object value) throws RuleException {
    DataType<?> dataType = column.getType();
    if (value instanceof String) {
      return dataType.fromString((String) value);
    }
    if (value != null && !value.getClass().equals(dataType.javaClass())) {
      throw new RuleException(String.format("Expected '%s' but got '%s' for '%s' (%s).",
          dataType, DataTypes.nameFromJavaClass(value.getClass()), column.getName(), value));
    }
    dataType.check(value);
    return value;
  }

  public void insert(Resources resources, Object[] rawValues, boolean checkReferencingRules)
      throws IOException, RuleException {
    long fileIndex = actFileIndex;
    Object[] values = new Object[rawValues.length];
    for (int i = 0; i < rawValues.length; i++) {
      BasicColumn column = activeColumns.get(i);
      values[i] = check(column, rawValues[i]);
      Object value = values[i];
      column.getType().check(value);
      if (!column.isNullable() && value == null) {
        throw new RuleException("Column is not nullable (mandatory).");
      }
      if (value != null) {
        if (column.isUnique()) {
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
    }
    byte[] data = Serializer.serialize(values, allColumns);
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
      int delta = 0;
      if (recordsToInsert.size() > 1) {
        ByteArrayOutputStream finalBuffer = new ByteArrayOutputStream(1024);
        for (byte[] data : recordsToInsert.values()) {
          finalBuffer.write(data);
        }
        byte[] data = finalBuffer.toByteArray();
        writer.write(fileIndex0, data);
        delta = data.length;
      } else {
        for (byte[] data : recordsToInsert.values()) {
          writer.write(fileIndex0, data);
          delta += data.length;
        }
      }
      for (Long pos : recordsToDelete) {
        writer.write(pos, new byte[] { 0 });
      }
      recordsToInsert.clear();
      recordsToDelete.clear();
      fileIndex0 += delta;
      actFileIndex = fileIndex0;
    } catch (IOException e) {
      rollback();
      writer.getChannel().truncate(fileIndex0);
      throw e;
    }
    for (TableUniqueIndex index : uniqueIndexes.values()) {
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
    actFileIndex = fileIndex0;
    for (TableUniqueIndex index : uniqueIndexes.values()) {
      index.rollback();
    }
    for (TableMultiIndex index : multiIndexes.values()) {
      index.rollback();
    }
  }

  public void close() throws IOException {
    writer.close();
  }

  public void cancel() {
    this.cancelled = true;
  }

  private void delete(Record record, Resources resources, Predicate<Integer> checkReversedForeignIndex,
      boolean checkReverseRuleDependencies)
      throws IOException, RuleException {
    recordsToDelete.add(record.getFilePointer());
    for (BasicColumn column : activeColumns) {
      Object value = record.getValues()[column.getIndex()];
      if (value != null) {
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
      if (record == RecordReader.EMPTY || cancelled) {
        recordProvider.close();
        return deletedLines;
      }
      if (!recordsToDelete.contains(record.getFilePointer())
          && (boolean) whereColumn.value(record.getValues(), resources, TableContext.EMPTY)) {
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
      if (record == RecordReader.EMPTY || cancelled) {
        recordProvider.close();
        return updatedRecords.build();
      }
      Object[] values = record.getValues();
      if (!recordsToDelete.contains(record.getFilePointer()) && (boolean) whereColumn.value(values, resources, TableContext.EMPTY)) {
        delete(
            record,
            resources,
            (columnIndex) -> updateExprs.containsKey(columnIndex),
            /* checkReverseRuleDependencies= */false);
        Object[] newValues = new Object[values.length];
        System.arraycopy(values, 0, newValues, 0, values.length);
        for (Map.Entry<Integer, ColumnMeta> updateExpr : updateExprs.entrySet()) {
          newValues[updateExpr.getKey()] = updateExpr.getValue().value(values, resources, TableContext.EMPTY);
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
        if (cancelled) {
          return -1;
        }
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
      if (cancelled) {
        return -1;
      }
    }
    return valuess.size();
  }

  public TableUniqueIndex getPrimaryKeyIndex() {
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

  public IndexWriter getIndexWriter(String name) {
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
    IndexReader index = resources.getPrimaryKeyIndex(tableMeta.fullName());
    long filePointer = index.get(key)[0];
    return get(filePointer);
  }

  private RecordReader recordReader() throws IOException {
    @SuppressWarnings("resource") // RecordReader closes SequenceInputStream.
    InputStream rafReader = new SequenceInputStream(
        new BufferedInputStream(new FileInputStream(fileName)),
        new TreeMapInputStream(recordsToInsert));
    return new RecordReader(allColumns, rafReader, recordsToDelete);
  }

  private MultiFilteredTableReader indexFilteredReader(Resources resources, ColumnMeta whereColumn,
      VariableEquals clause) {
    return new MultiFilteredTableReader(this, whereColumn, resources) {
      @Override
      protected void readPositions() throws IOException {
        IndexReader index = resources.getIndex(tableMeta.fullName(), clause.getIdent().getString());
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

  public void buildIndex(String column, IndexWriter indexWriter) throws IOException, RuleException {
    RecordReader reader = recordReader();
    int columnIndex = tableMeta.columnNames().asList().indexOf(column);
    Record record;
    try {
      while ((record = reader.read()) != RecordReader.EMPTY && !cancelled) {
        Object key = record.getValues()[columnIndex];
        if (key != null) {
          try {
            indexWriter.put(key, record.getFilePointer());
          } catch (IndexException e) {
            throw new RuleException(e);
          }
        }
      }
    } finally {
      reader.close();
    }
  }

  public void checkForeignKey(ForeignKey foreignKey, Resources resources) throws RuleException, IOException {
    RecordReader reader = recordReader();
    IndexReader index = resources.getPrimaryKeyIndex(foreignKey.getRefTable().fullName());
    int columnIndex = tableMeta.columnNames().asList().indexOf(foreignKey.getColumn().getName());
    Record record;
    try {
      while ((record = reader.read()) != RecordReader.EMPTY && !cancelled) {
        Object key = record.getValues()[columnIndex];
        if (key != null && !index.contains(key)) {
          throw new RuleException(
              String.format("Invalid key '%s' (value of '%s.%s'), not found in referenced table '%s.%s'.",
                  key,
                  foreignKey.getTable().fullName(),
                  foreignKey.getColumn().getName(),
                  foreignKey.getRefTable().fullName(),
                  foreignKey.getRefColumn().getName()));
        }
      }
    } finally {
      reader.close();
    }
  }

  public void checkRule(BooleanRule rule, Resources resources) throws IOException, RuleException {
    RecordReader reader = recordReader();
    Record record;
    try {
      while ((record = reader.read()) != RecordReader.EMPTY && !cancelled) {
        if (!rule.check(resources, record.getFilePointer())) {
          throw new RuleException(String.format("Constraint check %s failed.", rule.getName()));
        }
      }
    } finally {
      reader.close();
    }
  }
}
