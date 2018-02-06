package com.cosyan.db.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Aggregators.Aggregator;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.TableMeta.IterableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public abstract class AggrTables extends IterableTableMeta {

  public static class NotAggrTableException extends ModelException {
    private static final long serialVersionUID = 1L;

    public NotAggrTableException() {
      super("");
    }
  }

  public abstract class AggrTableReader extends IterableTableReader {

    protected IterableTableReader sourceReader;
    protected Iterator<Object[]> iterator;
    protected boolean aggregated;

    public AggrTableReader(IterableTableReader sourceReader) {
      this.sourceReader = sourceReader;
    }

    @Override
    public void close() throws IOException {
      sourceReader.close();
    }
  }

  protected ColumnMeta havingColumn;
  protected final ArrayList<AggrColumn> aggrColumns;

  public AggrTables() {
    this.aggrColumns = new ArrayList<>();
    this.havingColumn = ColumnMeta.TRUE_COLUMN;
  }

  protected int size() {
    return aggrColumns.size() + sourceTable().getKeyColumns().size();
  }

  @Override
  protected TableMeta getRefTable(Ident ident) throws ModelException {
    return null;
  }

  @Override
  public MetaResources readResources() {
    MetaResources resources = sourceTable().readResources();
    for (AggrColumn column : aggrColumns) {
      resources = resources.merge(column.readResources());
    }
    return resources;
  }

  public abstract KeyValueTableMeta sourceTable();

  @Override
  public Iterable<TableMeta> tableDeps() {
    return Iterables.concat(
        ImmutableSet.of(sourceTable()),
        sourceTable().tableDeps(),
        aggrColumns.stream().flatMap(column -> column.tables().stream()).collect(Collectors.toSet()));
  }

  public void addAggrColumn(AggrColumn aggrColumn) {
    aggrColumns.add(aggrColumn);
  }

  public void setHavingColumn(ColumnMeta havingColumn) {
    this.havingColumn = havingColumn;
  }

  @Override
  public IndexColumn getColumn(Ident ident) throws ModelException {
    return sourceTable().column(ident).shift(this, 0);
  }

  public static class KeyValueAggrTableMeta extends AggrTables {
    private final KeyValueTableMeta sourceTable;

    public KeyValueAggrTableMeta(
        KeyValueTableMeta sourceTable) {
      this.sourceTable = sourceTable;
    }

    @Override
    public KeyValueTableMeta sourceTable() {
      return sourceTable;
    }

    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return new AggrTableReader(sourceTable.reader(key, resources)) {
        @Override
        public Object[] next() throws IOException {
          if (!aggregated) {
            aggregate(resources);
          }
          Object[] values = null;
          do {
            if (!iterator.hasNext()) {
              return null;
            }
            values = iterator.next();
          } while (!(boolean) havingColumn.getValue(values, resources) && !cancelled);
          return values;
        }

        private ImmutableList<Object> getKeyValues(Object[] sourceValues, Resources resources) throws IOException {
          ImmutableList.Builder<Object> builder = ImmutableList.builder();
          for (Map.Entry<String, ? extends ColumnMeta> entry : sourceTable.getKeyColumns().entrySet()) {
            builder.add(entry.getValue().getValue(sourceValues, resources));
          }
          return builder.build();
        }

        private void aggregate(Resources resources) throws IOException {
          HashMap<ImmutableList<Object>, Aggregator<?, ?>[]> aggregatedValues = new HashMap<>();
          while (!cancelled) {
            Object[] sourceValues = sourceReader.next();
            if (sourceValues == null) {
              break;
            }
            ImmutableList<Object> keyValues = getKeyValues(sourceValues, resources);
            if (!aggregatedValues.containsKey(keyValues)) {
              Aggregator<?, ?>[] aggrValues = new Aggregator[aggrColumns.size()];
              int i = 0;
              for (AggrColumn column : aggrColumns) {
                aggrValues[i++] = column.getFunction().create();
              }
              aggregatedValues.put(keyValues, aggrValues);
            }
            Aggregator<?, ?>[] aggrValues = aggregatedValues.get(keyValues);
            int i = 0;
            for (AggrColumn column : aggrColumns) {
              aggrValues[i++].add(column.getInnerValue(sourceValues, resources));
            }
          }
          final Iterator<Entry<ImmutableList<Object>, Aggregator<?, ?>[]>> innerIterator = aggregatedValues.entrySet()
              .iterator();
          iterator = new Iterator<Object[]>() {

            @Override
            public boolean hasNext() {
              return innerIterator.hasNext();
            }

            @Override
            public Object[] next() {
              Object[] result = new Object[size()];
              Entry<ImmutableList<Object>, Aggregator<?, ?>[]> item = innerIterator.next();
              Object[] keys = item.getKey().toArray();
              System.arraycopy(keys, 0, result, 0, keys.length);
              for (int i = 0; i < item.getValue().length; i++) {
                result[keys.length + i] = item.getValue()[i].finish();
              }
              return result;
            }
          };
          aggregated = true;
        }
      };
    }
  }

  public static class GlobalAggrTableMeta extends AggrTables {
    private final KeyValueTableMeta sourceTable;

    public GlobalAggrTableMeta(
        KeyValueTableMeta sourceTable) {
      this.sourceTable = sourceTable;
    }

    @Override
    public KeyValueTableMeta sourceTable() {
      return sourceTable;
    }

    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return new AggrTableReader(sourceTable.reader(key, resources)) {

        @Override
        public Object[] next() throws IOException {
          if (!aggregated) {
            aggregate();
          }
          Object[] values = null;
          do {
            if (!iterator.hasNext()) {
              return null;
            }
            values = iterator.next();
          } while (!(boolean) havingColumn.getValue(values, resources) && !cancelled);
          return values;
        }

        protected void aggregate() throws IOException {
          Aggregator<?, ?>[] aggrValues = new Aggregator[size()];
          int i = 1;
          for (AggrColumn column : aggrColumns) {
            aggrValues[i++] = column.getFunction().create();
          }
          while (!cancelled) {
            Object[] sourceValues = sourceReader.next();
            if (sourceValues == null) {
              break;
            }
            i = 1;
            for (AggrColumn column : aggrColumns) {
              aggrValues[i++].add(column.getInnerValue(sourceValues, resources));
            }
          }
          Object[] result = new Object[size()];
          for (int j = 0; j < aggrColumns.size(); j++) {
            result[j + 1] = aggrValues[j + 1].finish();
          }

          iterator = ImmutableList.of(result).iterator();
          aggregated = true;
        }
      };
    }
  }
}
