package com.cosyan.db.model;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.DerivedTables.ShiftedTableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class JoinTables {

  public static enum JoinType {
    INNER, LEFT, RIGHT
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class JoinTableMeta extends ExposedTableMeta {
    private final JoinType joinType;
    private final ExposedTableMeta leftTable;
    private final ExposedTableMeta rightTable;
    private final ImmutableList<ColumnMeta> leftTableJoinColumns;
    private final ImmutableList<ColumnMeta> rightTableJoinColumns;

    private final ExposedTableMeta mainTable;
    private final ExposedTableMeta joinTable;
    private final ImmutableList<ColumnMeta> mainTableJoinColumns;
    private final ImmutableList<ColumnMeta> joinTableJoinColumns;
    private final boolean mainTableFirst;
    private final boolean innerJoin;

    public JoinTableMeta(JoinType joinType, ExposedTableMeta leftTable, ExposedTableMeta rightTable,
        ImmutableList<ColumnMeta> leftTableJoinColumns, ImmutableList<ColumnMeta> rightTableJoinColumns) {
      this.joinType = joinType;
      this.leftTable = leftTable;
      this.rightTable = rightTable;
      this.leftTableJoinColumns = leftTableJoinColumns;
      this.rightTableJoinColumns = rightTableJoinColumns;

      if (joinType == JoinType.INNER) {
        mainTable = leftTable;
        joinTable = rightTable;
        mainTableJoinColumns = leftTableJoinColumns;
        joinTableJoinColumns = rightTableJoinColumns;
        mainTableFirst = true;
        innerJoin = true;
      } else if (joinType == JoinType.LEFT) {
        mainTable = leftTable;
        joinTable = rightTable;
        mainTableJoinColumns = leftTableJoinColumns;
        joinTableJoinColumns = rightTableJoinColumns;
        mainTableFirst = true;
        innerJoin = false;
      } else if (joinType == JoinType.RIGHT) {
        mainTable = rightTable;
        joinTable = leftTable;
        mainTableJoinColumns = rightTableJoinColumns;
        joinTableJoinColumns = leftTableJoinColumns;
        mainTableFirst = false;
        innerJoin = false;
      } else {
        // TODO remove this and resolve in compilation time.
        throw new RuntimeException("Unknown join type '" + joinType.name() + "'.");
      }
    }

    @Override
    public ImmutableList<String> columnNames() {
      return ImmutableList.<String>builder()
          .addAll(leftTable.columnNames())
          .addAll(rightTable.columnNames())
          .build();
    }

    @Override
    public ImmutableList<DataType<?>> columnTypes() {
      return ImmutableList.<DataType<?>>builder()
          .addAll(leftTable.columnTypes())
          .addAll(rightTable.columnTypes())
          .build();
    }

    @Override
    public IndexColumn getColumn(Ident ident) throws ModelException {
      boolean presentInLeftTable = leftTable.hasColumn(ident);
      boolean presentInRightTable = rightTable.hasColumn(ident);
      if (presentInLeftTable && presentInRightTable) {
        throw new ModelException("Ambiguous column reference '" + ident + "'.", ident);
      }
      if (presentInLeftTable) {
        return leftTable.getColumn(ident).shift(this, 0);
      }
      if (presentInRightTable) {
        return rightTable.getColumn(ident).shift(this, leftTable.columnNames().size());
      }
      throw new ModelException("Column '" + ident + "' not found in table.", ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      boolean presentInLeftTable = leftTable.hasTable(ident);
      boolean presentInRightTable = rightTable.hasTable(ident);
      if (presentInLeftTable && presentInRightTable) {
        throw new ModelException("Ambiguous table reference '" + ident + "'.", ident);
      }
      if (presentInLeftTable) {
        return new ShiftedTableMeta(this, leftTable.getRefTable(ident), 0);
      }
      if (presentInRightTable) {
        return new ShiftedTableMeta(this, rightTable.getRefTable(ident), leftTable.columnNames().size());
      }
      throw new ModelException("Table reference '" + ident + "' not found.", ident);
    }

    @Override
    public MetaResources readResources() {
      return leftTable.readResources()
          .merge(rightTable.readResources())
          .merge(DerivedTables.resourcesFromColumns(leftTableJoinColumns))
          .merge(DerivedTables.resourcesFromColumns(rightTableJoinColumns));
    }

    @Override
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      final IterableTableReader mainReader = mainTable.reader(resources, context);
      final IterableTableReader joinReader = joinTable.reader(resources, context);
      return new IterableTableReader() {

        private boolean joined;
        private LinkedListMultimap<ImmutableList<Object>, Object[]> joinValues;
        private Iterator<Object[]> joinValuesForCurrentKey;
        private Object[] mainTableValues;

        @Override
        public void close() throws IOException {
          mainReader.close();
          joinReader.close();
        }

        @Override
        public Object[] next() throws IOException {
          if (!joined) {
            join();
          }
          Object[] result = null;
          do {
            while (joinValuesForCurrentKey == null || !joinValuesForCurrentKey.hasNext()) {
              List<Object[]> values = null;
              mainTableValues = mainReader.next();
              if (mainTableValues == null) {
                return null;
              }
              ImmutableList.Builder<Object> builder = ImmutableList.builder();
              for (ColumnMeta column : mainTableJoinColumns) {
                Object key = column.value(mainTableValues, resources, context);
                builder.add(key);
              }
              values = joinValues.get(builder.build());
              if (values != null && !values.isEmpty()) {
                joinValuesForCurrentKey = values.iterator();
              } else if (!innerJoin) {
                Object[] nullValues = new Object[joinTable.columnNames().size()];
                Arrays.fill(nullValues, null);
                joinValuesForCurrentKey = ImmutableList.of(nullValues).iterator();
              }
            }

            result = match(mainTableValues, joinValuesForCurrentKey.next());
          } while (result == null);

          return result;
        }

        private Object[] match(Object[] mainTableValues, Object[] joinTableValues) {
          Object[] result = new Object[mainTable.columnNames().size() + joinTable.columnNames().size()];
          if (joinTableValues == null) {
            return null;
          }
          if (mainTableFirst) {
            System.arraycopy(mainTableValues, 0, result, 0, mainTableValues.length);
            System.arraycopy(joinTableValues, 0, result, mainTableValues.length, joinTableValues.length);
          } else {
            System.arraycopy(joinTableValues, 0, result, 0, joinTableValues.length);
            System.arraycopy(mainTableValues, 0, result, joinTableValues.length, mainTableValues.length);
          }
          return result;
        }

        private void join() throws IOException {
          joinValues = LinkedListMultimap.create();
          while (!cancelled.get()) {
            Object[] joinSourceValues = joinReader.next();
            if (joinSourceValues == null) {
              break;
            }
            ImmutableList.Builder<Object> builder = ImmutableList.builder();
            for (ColumnMeta column : joinTableJoinColumns) {
              Object key = column.value(joinSourceValues, resources, context);
              builder.add(key);
            }
            joinValues.put(builder.build(), joinSourceValues);
          }
          joined = true;
        }
      };
    }

    @Override
    public TableDependencies tableDependencies() {
      TableDependencies deps = new TableDependencies();
      deps.addToThis(mainTable.tableDependencies());
      deps.addToThis(joinTable.tableDependencies());
      return deps;
    }
  }
}
