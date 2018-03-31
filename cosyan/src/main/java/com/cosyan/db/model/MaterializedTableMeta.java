package com.cosyan.db.model;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.cosyan.db.conf.Config;
import com.cosyan.db.io.MemoryBufferedSeekableFileStream;
import com.cosyan.db.io.RAFBufferedInputStream;
import com.cosyan.db.io.RecordProvider.Record;
import com.cosyan.db.io.SeekableInputStream;
import com.cosyan.db.io.SeekableOutputStream;
import com.cosyan.db.io.SeekableOutputStream.RAFSeekableOutputStream;
import com.cosyan.db.io.TableReader.DerivedIterableTableReader;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.TableProvider;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
import com.cosyan.db.model.Dependencies.ReverseRuleDependencies;
import com.cosyan.db.model.Dependencies.ReverseRuleDependency;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.Dependencies.TableDependency;
import com.cosyan.db.model.Dependencies.TransitiveTableDependency;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.References.ReferencingTable;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class MaterializedTableMeta {

  private final Config config;
  private final String tableName;
  private final RandomAccessFile raf;
  private final SeekableOutputStream fileWriter;
  private final List<BasicColumn> columns;
  private final Map<String, BooleanRule> rules;
  private Optional<PrimaryKey> primaryKey;
  private final Map<String, ForeignKey> foreignKeys;
  private final Map<String, ReverseForeignKey> reverseForeignKeys;
  private final Map<String, TableRef> refs;
  private TableDependencies ruleDependencies;
  private ReverseRuleDependencies reverseRuleDependencies;
  private Optional<ColumnMeta> partitioning;
  private long cnt;

  public MaterializedTableMeta(
      Config config,
      String tableName,
      Iterable<BasicColumn> columns,
      Optional<PrimaryKey> primaryKey) throws IOException {
    this.config = config;
    this.tableName = tableName;
    this.raf = new RandomAccessFile(fileName(), "rw");
    this.columns = Lists.newArrayList(columns);
    this.primaryKey = primaryKey;
    this.rules = new HashMap<>();
    this.foreignKeys = new HashMap<>();
    this.reverseForeignKeys = new HashMap<>();
    this.refs = new HashMap<>();
    this.ruleDependencies = new TableDependencies();
    this.reverseRuleDependencies = new ReverseRuleDependencies();
    this.partitioning = Optional.empty();
    this.cnt = 0;

    if (true) {
      fileWriter = new RAFSeekableOutputStream(raf);
    } else {
      MemoryBufferedSeekableFileStream mbsfs = new MemoryBufferedSeekableFileStream(raf);
      fileWriter = mbsfs;
    }
  }

  public String fileName() {
    return config.tableDir() + File.separator + tableName();
  }

  public RandomAccessFile raf() {
    return raf;
  }

  public SeekableOutputStream fileWriter() {
    return fileWriter;
  }

  public SeekableInputStream fileReader() throws IOException {
    return new RAFBufferedInputStream(raf);
  }

  public ImmutableList<String> columnNames() {
    return columnsMap(columns).keySet().asList();
  }

  public ImmutableMap<String, BasicColumn> columns() {
    return columnsMap(columns);
  }

  public BasicColumn pkColumn() throws ModelException {
    if (!primaryKey.isPresent()) {
      throw new ModelException(String.format("Table '%s' has no primary key.", tableName));
    }
    return primaryKey.get().getColumn();
  }

  public static ImmutableMap<String, BasicColumn> columnsMap(List<BasicColumn> columns) {
    // Need to keep iteration order;
    ImmutableMap.Builder<String, BasicColumn> builder = ImmutableMap.builder();
    for (BasicColumn column : columns) {
      if (!column.isDeleted()) {
        builder.put(column.getName(), column);
      }
    }
    return builder.build();
  }

  public ImmutableList<BasicColumn> allColumns() {
    return ImmutableList.copyOf(columns);
  }

  public BasicColumn column(Ident ident) throws ModelException {
    if (!columns().containsKey(ident.getString())) {
      throw new ModelException(String.format("Column '%s' not found in table '%s'.", ident, tableName));
    }
    return columns().get(ident.getString());
  }

  public boolean hasColumn(Ident ident) {
    try {
      return column(ident) != null;
    } catch (ModelException e) {
      return false;
    }
  }

  public String tableName() {
    return tableName;
  }

  public Map<String, BooleanRule> rules() {
    return Collections.unmodifiableMap(rules);
  }

  public Map<String, TableDependency> ruleDependencies() {
    return Collections.unmodifiableMap(ruleDependencies.getDeps());
  }

  public ReverseRuleDependencies reverseRuleDependencies() {
    return reverseRuleDependencies;
  }

  public Optional<PrimaryKey> primaryKey() {
    return primaryKey;
  }

  public Map<String, ForeignKey> foreignKeys() {
    return Collections.unmodifiableMap(foreignKeys);
  }

  public Map<String, ReverseForeignKey> reverseForeignKeys() {
    return Collections.unmodifiableMap(reverseForeignKeys);
  }

  public Map<String, TableRef> refs() {
    return Collections.unmodifiableMap(refs);
  }

  private void checkName(String name) throws ModelException {
    if (columnNames().contains(name) || foreignKeys.containsKey(name) || reverseForeignKeys.containsKey(name)) {
      throw new ModelException(
          String.format("Duplicate column, foreign key or reversed foreign key name in '%s': '%s'.", tableName, name));
    }
  }

  public void addForeignKey(ForeignKey foreignKey) throws ModelException {
    checkName(foreignKey.getName());
    foreignKey.getRefTable().checkName(foreignKey.getName());
    foreignKeys.put(foreignKey.getName(), foreignKey);
    foreignKey.getRefTable().reverseForeignKeys.put(foreignKey.getRevName(), foreignKey.createReverse());
  }

  public void addColumn(BasicColumn basicColumn) throws ModelException {
    checkName(basicColumn.getName());
    columns.add(basicColumn);
  }

  public void addRef(TableRef ref) throws ModelException {
    checkName(ref.getName());
    refs.put(ref.getName(), ref);
  }

  public void addRule(BooleanRule rule) throws ModelException {
    checkName(rule.name());
    rules.put(rule.name(), rule);
    ruleDependencies.addToThis(rule.getDeps());
    rule.getDeps().addAllReverseRuleDependencies(rule);
  }

  public void addReverseRuleDependency(Iterable<Ref> reverseForeignKeyChain, BooleanRule rule) {
    reverseRuleDependencies.addReverseRuleDependency(reverseForeignKeyChain, rule);
  }

  public Optional<ColumnMeta> getPartitioning() {
    return partitioning;
  }

  public void setPartitioning(Optional<ColumnMeta> partitioning) {
    this.partitioning = partitioning;
  }

  public ForeignKey foreignKey(String name) throws ModelException {
    if (!foreignKeys.containsKey(name)) {
      throw new ModelException(String.format("Invalid foreign key reference '%s' in table '%s'.", name, tableName));
    }
    return foreignKeys.get(name);
  }

  public boolean hasForeignKey(String name) {
    return foreignKeys.containsKey(name);
  }

  public ReverseForeignKey reverseForeignKey(String name) throws ModelException {
    if (!reverseForeignKeys.containsKey(name)) {
      throw new ModelException(
          String.format("Invalid reverse foreign key reference '%s' in table '%s'.", name, tableName));
    }
    return reverseForeignKeys.get(name);
  }

  public boolean hasReverseForeignKey(String name) {
    return reverseForeignKeys.containsKey(name);
  }

  public MetaResources ruleDependenciesReadResources() {
    return readResources(ruleDependencies.getDeps().values());
  }

  public MetaResources reverseRuleDependenciesReadResources() {
    MetaResources readResources = MetaResources.readTable(this);
    for (ReverseRuleDependency dependency : reverseRuleDependencies.getDeps().values()) {
      readResources = readResources.merge(readResources(dependency));
    }
    return readResources;
  }

  public static MetaResources readResources(Iterable<? extends TransitiveTableDependency> deps) {
    MetaResources readResources = MetaResources.empty();
    for (TransitiveTableDependency dependency : deps) {
      readResources = readResources.merge(readResources(dependency));
    }
    return readResources;
  }

  public static MetaResources readResources(TransitiveTableDependency dependency) {
    MetaResources readResources = MetaResources.readTable(dependency.table());
    for (TransitiveTableDependency childDep : dependency.childDeps()) {
      readResources = readResources.merge(readResources(childDep));
    }
    return readResources;
  }

  public SeekableTableMeta reader() {
    return new SeekableTableMeta(this);
  }

  public MetaResources readResources() {
    return MetaResources.readTable(this);
  }

  public boolean isEmpty() {
    return cnt == 0;
  }

  public static class SeekableTableMeta extends ExposedTableMeta implements ReferencingTable, TableProvider {

    private final MaterializedTableMeta tableMeta;

    public SeekableTableMeta(MaterializedTableMeta tableMeta) {
      this.tableMeta = tableMeta;
    }

    public Record get(Resources resources, long position) throws IOException {
      return resources.reader(tableName()).get(position);
    }

    @Override
    public ImmutableList<String> columnNames() {
      return tableMeta.columnNames();
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      BasicColumn column = tableMeta.column(ident);
      if (column == null) {
        return null;
      }
      int index = tableMeta.columnNames().indexOf(column.getName());
      return new IndexColumn(this, index, column.getType(), new TableDependencies());
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return References.getRefTable(
          this,
          tableMeta.tableName(),
          ident.getString(),
          tableMeta.foreignKeys(),
          tableMeta.reverseForeignKeys(),
          tableMeta.refs());
    }

    @Override
    public MetaResources readResources() {
      return MetaResources.readTable(tableMeta);
    }

    public String tableName() {
      return tableMeta.tableName();
    }

    public MaterializedTableMeta tableMeta() {
      return tableMeta;
    }

    @Override
    public Iterable<Ref> foreignKeyChain() {
      return ImmutableList.of();
    }

    @Override
    public ReferencingTable getParent() {
      return null;
    }

    @Override
    public ExposedTableMeta tableMeta(Ident ident) throws ModelException {
      if (tableMeta.hasReverseForeignKey(ident.getString())) {
        return new ReferencedMultiTableMeta(this, tableMeta.reverseForeignKey(ident.getString()));
      } else {
        throw new ModelException(String.format("Table '%s' not found.", ident.getString()));
      }
    }

    @Override
    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return new DerivedIterableTableReader(resources.createIterableReader(tableName())) {

        @Override
        public Object[] next() throws IOException {
          return sourceReader.next();
        }
      };
    }
  }
}
