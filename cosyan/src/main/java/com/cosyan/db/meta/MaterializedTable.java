package com.cosyan.db.meta;

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
import com.cosyan.db.io.SeekableInputStream;
import com.cosyan.db.io.SeekableOutputStream;
import com.cosyan.db.io.SeekableOutputStream.RAFSeekableOutputStream;
import com.cosyan.db.lang.expr.TableDefinition.ColumnDefinition;
import com.cosyan.db.lang.expr.TableDefinition.ForeignKeyDefinition;
import com.cosyan.db.lang.expr.TableDefinition.RefDefinition;
import com.cosyan.db.lang.expr.TableDefinition.RuleDefinition;
import com.cosyan.db.lang.sql.SelectStatement;
import com.cosyan.db.lang.sql.SelectStatement.Select.TableColumns;
import com.cosyan.db.meta.Dependencies.ReverseRuleDependencies;
import com.cosyan.db.meta.Dependencies.ReverseRuleDependency;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.Dependencies.TableDependency;
import com.cosyan.db.meta.Dependencies.TransitiveTableDependency;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.References.RefTableMeta;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.SeekableTableMeta;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableRef;
import com.cosyan.db.model.AggrTables.GlobalAggrTableMeta;
import com.cosyan.db.model.DerivedTables.FilteredTableMeta;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.stat.TableStats;
import com.cosyan.db.transaction.MetaResources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class MaterializedTable {

  public static enum Type {
    LOG, LOOKUP
  }

  private final Config config;
  private final String tableName;
  private final String owner;
  private final Type type;
  private final RandomAccessFile raf;
  private final TableStats stats;
  private final SeekableOutputStream fileWriter;
  private final List<BasicColumn> columns;
  private final Map<String, BooleanRule> rules;
  private final Optional<PrimaryKey> primaryKey;
  private final Map<String, ForeignKey> foreignKeys;
  private final Map<String, ReverseForeignKey> reverseForeignKeys;
  private final Map<String, TableRef> refs;
  private TableDependencies ruleDependencies;
  private ReverseRuleDependencies reverseRuleDependencies;
  private Optional<ColumnMeta> partitioning;
  private SeekableInputStream fileReader;
  private boolean isEmpty;

  public MaterializedTable(
      Config config,
      String tableName,
      String owner,
      Iterable<BasicColumn> columns,
      Optional<PrimaryKey> primaryKey,
      Type type) throws IOException, ModelException {
    this.config = config;
    this.tableName = tableName;
    this.owner = owner;
    this.type = type;
    this.raf = new RandomAccessFile(fileName(), "rw");
    this.isEmpty = raf.length() == 0L;
    this.stats = new TableStats(config, tableName);
    this.columns = Lists.newArrayList(columns);
    this.primaryKey = primaryKey;
    this.rules = new HashMap<>();
    this.foreignKeys = new HashMap<>();
    this.reverseForeignKeys = new HashMap<>();
    this.refs = new HashMap<>();
    this.ruleDependencies = new TableDependencies();
    this.reverseRuleDependencies = new ReverseRuleDependencies();
    this.partitioning = Optional.empty();

    if (type == Type.LOG) {
      fileWriter = new RAFSeekableOutputStream(raf);
      fileReader = new RAFBufferedInputStream(raf);
    } else {
      MemoryBufferedSeekableFileStream mbsfs = new MemoryBufferedSeekableFileStream(raf);
      fileWriter = mbsfs;
      fileReader = mbsfs;
    }
  }

  public String fileName() {
    return config.tableDir() + File.separator + tableName();
  }

  public Type type() {
    return type;
  }

  public String owner() {
    return owner;
  }

  public RandomAccessFile raf() {
    return raf;
  }

  public SeekableOutputStream fileWriter() {
    return fileWriter;
  }

  public SeekableInputStream fileReader() throws IOException {
    fileReader.reset();
    return fileReader;
  }

  public void loadStats() throws IOException {
    stats.load();
  }

  public void saveStats() throws IOException {
    stats.save();
  }

  public ImmutableList<String> columnNames() {
    return columnsMap(columns).keySet().asList();
  }

  public ImmutableMap<String, BasicColumn> columns() {
    return columnsMap(columns);
  }

  public Optional<BasicColumn> pkColumn() throws ModelException {
    return primaryKey.map(pk -> pk.getColumn());
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
      throw new ModelException(String.format("Column '%s' not found in table '%s'.", ident, tableName), ident);
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

  public void checkName(Ident ident) throws ModelException {
    String name = ident.getString();
    if (columnNames().contains(name) || foreignKeys.containsKey(name) || reverseForeignKeys.containsKey(name)) {
      throw new ModelException(
          String.format("Duplicate column, foreign key or reversed foreign key name in '%s': '%s'.", tableName, name),
          ident);
    }
  }

  private void assertName(String name) {
    assert !columnNames().contains(name) && !foreignKeys.containsKey(name) && !reverseForeignKeys.containsKey(name);
  }

  public void insert(int insertedLines) {
    isEmpty = false;
    stats.insert(insertedLines);
  }

  public void delete(long deletedLines) {
    stats.delete(deletedLines);
  }

  public TableStats stats() {
    return stats;
  }

  public ForeignKey createForeignKey(ForeignKeyDefinition foreignKeyDefinition, MaterializedTable refTable)
      throws ModelException {
    checkName(foreignKeyDefinition.getName());
    refTable.checkName(foreignKeyDefinition.getRevName());
    BasicColumn keyColumn = column(foreignKeyDefinition.getKeyColumn());
    Ident refTableName = foreignKeyDefinition.getRefTable();

    Optional<BasicColumn> refColumnOpt = refTable.pkColumn();
    if (!refColumnOpt.isPresent()) {
      throw new ModelException(String.format("Table '%s' has no primary key.", refTableName), refTableName);
    }
    BasicColumn refColumn = refColumnOpt.get();

    if (foreignKeyDefinition.getRefColumn().isPresent()
        && !foreignKeyDefinition.getRefColumn().get().getString().equals(refColumn.getName())) {
      throw new ModelException(
          "Foreign key reference column has to be the primary key column of the referenced table.",
          foreignKeyDefinition.getRefColumn().get());
    }
    if (keyColumn.getType() != refColumn.getType()
        && !(keyColumn.getType() == DataTypes.LongType && refColumn.getType() == DataTypes.IDType)) {
      throw new ModelException(
          String.format("Foreign key reference column has type '%s' while key column has type '%s'.",
              refColumn.getType(), keyColumn.getType()),
          foreignKeyDefinition.getName());
    }
    assert refColumn.isUnique() && !refColumn.isNullable();
    return new ForeignKey(
        foreignKeyDefinition.getName().getString(),
        foreignKeyDefinition.getRevName().getString(),
        this,
        keyColumn,
        refTable,
        refColumn);
  }

  public void addForeignKey(ForeignKey foreignKey) {
    foreignKeys.put(foreignKey.getName(), foreignKey);
  }

  public void addReverseForeignKey(ReverseForeignKey reverseForeignKey) {
    ReverseForeignKey existingKey = reverseForeignKeys.get(reverseForeignKey.getName());
    if (existingKey == null) {
      reverseForeignKeys.put(reverseForeignKey.getName(), reverseForeignKey);
    } else {
      assert reverseForeignKey.equals(existingKey);
    }
  }

  public BasicColumn createColumn(ColumnDefinition columnDefiniton) throws ModelException {
    checkName(columnDefiniton.getName());
    BasicColumn column = new BasicColumn(
        allColumns().size(),
        columnDefiniton.getName(),
        columnDefiniton.getType(),
        columnDefiniton.isNullable(),
        columnDefiniton.isUnique(),
        columnDefiniton.isImmutable());

    assert column.getIndex() == columns.size();
    if (!isEmpty && !column.isNullable()) {
      throw new ModelException(
          String.format("Cannot add column '%s', new columns on a non empty table have to be nullable.",
              column.getName()),
          columnDefiniton.getName());
    }
    return column;
  }

  public void addColumn(BasicColumn column) {
    assertName(column.getName());
    columns.add(column);
  }

  public RefTableMeta createRef(RefDefinition ref) throws ModelException {
    checkName(ref.getName());
    ReferencedMultiTableMeta srcTableMeta = (ReferencedMultiTableMeta) ref.getSelect().getTable()
        .compile(reader());
    ExposedTableMeta derivedTable;
    if (ref.getSelect().getWhere().isPresent()) {
      ColumnMeta whereColumn = ref.getSelect().getWhere().get().compileColumn(srcTableMeta);
      derivedTable = new FilteredTableMeta(srcTableMeta, whereColumn);
    } else {
      derivedTable = srcTableMeta;
    }
    GlobalAggrTableMeta aggrTable = new GlobalAggrTableMeta(
        new KeyValueTableMeta(
            derivedTable,
            TableMeta.wholeTableKeys));
    // Columns have aggregations, recompile with an AggrTable.
    TableColumns tableColumns = SelectStatement.Select.tableColumns(aggrTable, ref.getSelect().getColumns());
    return new RefTableMeta(
        aggrTable, tableColumns.getColumns(), srcTableMeta.getReverseForeignKey());
  }
  
  public void addRef(TableRef ref) {
    assertName(ref.getName());
    refs.put(ref.getName(), ref);
  }

  public BooleanRule createRule(RuleDefinition ruleDefinition) throws ModelException {
    checkName(ruleDefinition.getName());
    Rule rule = ruleDefinition.compile(this);
    if (rule.getType() != DataTypes.BoolType) {
      throw new ModelException(
          String.format("Constraint check expression has to return a 'boolean': '%s'.", rule.getExpr().print()),
          ruleDefinition.getName());
    }
    return rule.toBooleanRule();
  }

  public void addRule(BooleanRule booleanRule) {
    rules.put(booleanRule.getName(), booleanRule);
    ruleDependencies.addToThis(booleanRule.getDeps());
  }

  void addReverseRuleDependency(Iterable<Ref> reverseForeignKeyChain, BooleanRule rule) {
    reverseRuleDependencies.addReverseRuleDependency(reverseForeignKeyChain, rule);
  }

  public Optional<ColumnMeta> getPartitioning() {
    return partitioning;
  }

  public void setPartitioning(Optional<ColumnMeta> partitioning) {
    this.partitioning = partitioning;
  }

  public ForeignKey foreignKey(Ident ident) throws ModelException {
    String name = ident.getString();
    if (!foreignKeys.containsKey(name)) {
      throw new ModelException(String.format("Invalid foreign key reference '%s' in table '%s'.", name, tableName),
          ident);
    }
    return foreignKeys.get(name);
  }

  public boolean hasForeignKey(String name) {
    return foreignKeys.containsKey(name);
  }

  public ReverseForeignKey reverseForeignKey(Ident ident) throws ModelException {
    String name = ident.getString();
    if (!reverseForeignKeys.containsKey(name)) {
      throw new ModelException(
          String.format("Invalid reverse foreign key reference '%s' in table '%s'.", name, tableName), ident);
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
    return stats.isEmpty();
  }

  public void checkDeleteColumn(Ident column) throws ModelException {
    BasicColumn basicColumn = column(column);
    basicColumn.setDeleted(true);
    try {
      for (ForeignKey foreignKey : foreignKeys().values()) {
        if (foreignKey.getColumn().getName().equals(basicColumn.getName())) {
          throw new ModelException(String.format(
              "Cannot drop column '%s', it is used by foreign key '%s'.", column, foreignKey), column);
        }
      }
      for (ReverseForeignKey foreignKey : reverseForeignKeys().values()) {
        if (foreignKey.getColumn().getName().equals(basicColumn.getName())) {
          throw new ModelException(String.format(
              "Cannot drop column '%s', it is used by reverse foreign key '%s'.", column, foreignKey), column);
        }
      }
      for (Rule rule : rules().values()) {
        try {
          rule.reCompile(this);
        } catch (ModelException e) {
          throw new ModelException(String.format(
              "Cannot drop column '%s', check '%s' fails.\n%s", column, rule, e.getMessage()), column);
        }
      }
      for (Rule rule : reverseRuleDependencies().allRules()) {
        try {
          rule.reCompile(this);
        } catch (ModelException e) {
          throw new ModelException(String.format(
              "Cannot drop column '%s', check '%s' fails.\n%s", column, rule, e.getMessage()), column);
        }
      }
    } finally {
      basicColumn.setDeleted(false);
    }
  }

  public void drop() throws IOException {
    raf.close();
    new File(fileName()).delete();
  }
}
