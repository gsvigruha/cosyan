package com.cosyan.db.meta;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.auth.LocalUsers;
import com.cosyan.db.conf.Config;
import com.cosyan.db.index.ByteMultiTrie.LongMultiIndex;
import com.cosyan.db.index.ByteMultiTrie.StringMultiIndex;
import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.ByteTrie.LongIndex;
import com.cosyan.db.index.ByteTrie.StringIndex;
import com.cosyan.db.index.IDIndex;
import com.cosyan.db.index.IndexStat.ByteMultiTrieStat;
import com.cosyan.db.index.IndexStat.ByteTrieStat;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.Indexes.IndexWriter;
import com.cosyan.db.io.MetaSerializer;
import com.cosyan.db.io.TableReader.MaterializedTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.lock.LockManager;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.Grants.GrantToken;
import com.cosyan.db.meta.Grants.Method;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMultiIndex;
import com.cosyan.db.model.TableMultiIndex.LongTableMultiIndex;
import com.cosyan.db.model.TableMultiIndex.StringTableMultiIndex;
import com.cosyan.db.model.TableUniqueIndex;
import com.cosyan.db.model.TableUniqueIndex.IDTableIndex;
import com.cosyan.db.model.TableUniqueIndex.LongTableIndex;
import com.cosyan.db.model.TableUniqueIndex.StringTableIndex;
import com.cosyan.db.session.ILexer;
import com.cosyan.db.session.IParser;
import com.cosyan.db.session.IParser.ParserException;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.MetaResources.TableMetaResource;
import com.cosyan.db.transaction.Resources;
import com.cosyan.db.util.Util;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

public class MetaRepo implements TableProvider, MetaRepoExecutor {

  private final Config config;
  private final HashMap<String, MaterializedTable> tables;
  private final HashMap<String, TableUniqueIndex> uniqueIndexes;
  private final HashMap<String, TableMultiIndex> multiIndexes;
  private final Grants grants;

  private final LockManager lockManager;
  private final MetaSerializer metaSerializer;

  public MetaRepo(Config config, LockManager lockManager, LocalUsers localUsers, ILexer lexer, IParser parser)
      throws IOException, DBException {
    this.config = config;
    this.lockManager = lockManager;
    this.metaSerializer = new MetaSerializer(lexer, parser);
    this.tables = new HashMap<>();
    this.uniqueIndexes = new HashMap<>();
    this.multiIndexes = new HashMap<>();
    this.grants = new Grants(localUsers);

    Files.createDirectories(Paths.get(config.tableDir()));
    Files.createDirectories(Paths.get(config.indexDir()));
    Files.createDirectories(Paths.get(config.journalDir()));
    Files.createDirectories(Paths.get(config.metaDir()));

    readTables();
  }

  public Config config() {
    return config;
  }

  public void init() throws IOException {
    for (MaterializedTable tableMeta : tables.values()) {
      tableMeta.loadStats();
    }
  }

  public void shutdown() throws IOException {
    for (MaterializedTable tableMeta : tables.values()) {
      tableMeta.saveStats();
    }
  }

  public void writeTables() throws IOException {
    for (MaterializedTable table : tables.values()) {
      JSONObject obj = metaSerializer.toJSON(table);
      FileUtils.writeStringToFile(
          new File(config.metaDir() + File.separator + table.tableName()),
          obj.toString(),
          Charset.defaultCharset());
    }
    FileUtils.writeStringToFile(
        new File(config.usersFile()),
        grants.toJSON().toString(),
        Charset.defaultCharset());
  }

  public void resetAndReadTables() throws DBException {
    this.uniqueIndexes.clear();
    this.multiIndexes.clear();
    readTables();
  }

  public void readTables() throws DBException {
    Map<String, MaterializedTable> newTables;
    try {
      File file = new File(config.metaDir());
      Map<String, JSONObject> jsons = new HashMap<>();
      for (String fileName : file.list()) {
        JSONObject json = new JSONObject(FileUtils.readFileToString(
            new File(config.metaDir() + File.separator + fileName),
            Charset.defaultCharset()));
        jsons.put(fileName, json);
      }
      newTables = metaSerializer.loadTables(config, jsons);
      grants.fromJSON(new JSONArray(FileUtils.readFileToString(
          new File(config.usersFile()),
          Charset.defaultCharset())), newTables);
    } catch (IOException | ParserException | ModelException | JSONException e) {
      throw new DBException(e);
    }
    this.tables.clear();
    this.tables.putAll(newTables);
    lockManager.syncLocks(tables.keySet());
    try {
      for (MaterializedTable table : tables.values()) {
        syncIndex(table);
      }
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  @Override
  public ExposedTableMeta tableMeta(Ident ident) throws ModelException {
    return table(ident).reader();
  }

  @Override
  public TableProvider tableProvider(Ident ident) throws ModelException {
    return table(ident).reader();
  }

  public MaterializedTable table(String name) throws ModelException {
    assert tables.containsKey(name) : String.format("Table '%s' does not exist.", name);
    return tables.get(name);
  }

  public MaterializedTable table(Ident ident) throws ModelException {
    if (!tables.containsKey(ident.getString())) {
      throw new ModelException("Table '" + ident.getString() + "' does not exist.", ident);
    }
    return tables.get(ident.getString());
  }

  public ImmutableMap<String, IndexReader> collectIndexReaders(MaterializedTable table) {
    ImmutableMap.Builder<String, IndexReader> builder = ImmutableMap.builder();
    for (BasicColumn column : table.columns().values()) {
      if (column.isIndexed()) {
        String indexName = table.tableName() + "." + column.getName();
        if (column.isUnique()) {
          builder.put(column.getName(), uniqueIndexes.get(indexName));
        } else {
          builder.put(column.getName(), multiIndexes.get(indexName));
        }
      }
    }
    return builder.build();
  }

  @VisibleForTesting
  public ImmutableMap<String, TableUniqueIndex> collectUniqueIndexes(MaterializedTable table) {
    ImmutableMap.Builder<String, TableUniqueIndex> builder = ImmutableMap.builder();
    for (BasicColumn column : table.columns().values()) {
      String indexName = table.tableName() + "." + column.getName();
      if (column.isIndexed() && column.isUnique()) {
        builder.put(column.getName(), uniqueIndexes.get(indexName));
      }
    }
    return builder.build();
  }

  @VisibleForTesting
  public ImmutableMap<String, TableMultiIndex> collectMultiIndexes(MaterializedTable table) {
    ImmutableMap.Builder<String, TableMultiIndex> builder = ImmutableMap.builder();
    for (BasicColumn column : table.columns().values()) {
      String indexName = table.tableName() + "." + column.getName();
      if (column.isIndexed() && !column.isUnique()) {
        builder.put(column.getName(), multiIndexes.get(indexName));
      }
    }
    return builder.build();
  }

  public ImmutableMultimap<String, IndexReader> collectForeignIndexes(MaterializedTable table) {
    ImmutableMultimap.Builder<String, IndexReader> builder = ImmutableMultimap.builder();
    for (ForeignKey foreignKey : table.foreignKeys().values()) {
      builder.put(
          foreignKey.getColumn().getName(),
          uniqueIndexes.get(foreignKey.getRefTable().tableName() + "." + foreignKey.getRefColumn().getName()));
    }
    return builder.build();
  }

  public ImmutableMultimap<String, IndexReader> collectReverseForeignIndexes(MaterializedTable table) {
    ImmutableMultimap.Builder<String, IndexReader> builder = ImmutableMultimap.builder();
    for (ReverseForeignKey reverseForeignKey : table.reverseForeignKeys().values()) {
      String indexName = reverseForeignKey.getRefTable().tableName() + "."
          + reverseForeignKey.getRefColumn().getName();
      if (reverseForeignKey.getRefColumn().isUnique()) {
        builder.put(reverseForeignKey.getColumn().getName(), uniqueIndexes.get(indexName));
      } else {
        builder.put(reverseForeignKey.getColumn().getName(), multiIndexes.get(indexName));
      }
    }
    return builder.build();
  }

  public void syncIndex(MaterializedTable tableMeta) throws IOException {
    for (BasicColumn column : tableMeta.allColumns()) {
      if (column.isIndexed()) {
        if (column.isDeleted()) {
          if (column.isUnique()) {
            dropUniqueIndex(tableMeta, column);
          } else {
            dropMultiIndex(tableMeta, column);
          }
        } else {
          registerIndex(tableMeta, column);
        }
      }
    }
  }

  @Override
  public void syncMeta(MaterializedTable tableMeta) {
    for (ForeignKey foreignKey : tableMeta.foreignKeys().values()) {
      foreignKey.getRefTable().addReverseForeignKey(foreignKey.createReverse());
    }
    for (BooleanRule rule : tableMeta.rules().values()) {
      rule.getDeps().forAllReverseRuleDependencies(rule, /* add= */true);
    }
  }

  public void registerTable(MaterializedTable tableMeta) throws IOException {
    String tableName = tableMeta.tableName();
    syncIndex(tableMeta);
    syncMeta(tableMeta);
    tables.put(tableName, tableMeta);
    lockManager.registerLock(tableName);
  }

  public void dropTable(MaterializedTable tableMeta, AuthToken authToken) throws IOException, GrantException {
    String tableName = tableMeta.tableName();
    grants.checkOwner(tableMeta, authToken);
    tables.remove(tableName);
    tableMeta.drop();
    for (BasicColumn column : tableMeta.allColumns()) {
      if (column.isIndexed()) {
        dropIndex(tableMeta, column, authToken);
      }
    }
    lockManager.removeLock(tableName);
  }

  public boolean hasTable(String tableName) {
    return tables.containsKey(tableName);
  }

  private TableUniqueIndex registerUniqueIndex(MaterializedTable table, BasicColumn column)
      throws IOException {
    String indexName = table.tableName() + "." + column.getName();
    String path = config.indexDir() + File.separator + indexName;
    if (!uniqueIndexes.containsKey(indexName)) {
      if (column.getType() == DataTypes.StringType) {
        uniqueIndexes.put(indexName, new StringTableIndex(new StringIndex(path)));
      } else if (column.getType() == DataTypes.LongType) {
        uniqueIndexes.put(indexName, new LongTableIndex(new LongIndex(path)));
      } else if (column.getType() == DataTypes.IDType) {
        uniqueIndexes.put(indexName, new IDTableIndex(new IDIndex(path)));
      }
    }
    return uniqueIndexes.get(indexName);
  }

  private TableMultiIndex registerMultiIndex(MaterializedTable table, BasicColumn column)
      throws IOException {
    String indexName = table.tableName() + "." + column.getName();
    String path = config.indexDir() + File.separator + indexName;
    if (!multiIndexes.containsKey(indexName)) {

      if (column.getType() == DataTypes.StringType) {
        multiIndexes.put(indexName, new StringTableMultiIndex(new StringMultiIndex(path)));
      } else if (column.getType() == DataTypes.LongType || column.getType() == DataTypes.IDType) {
        multiIndexes.put(indexName, new LongTableMultiIndex(new LongMultiIndex(path)));
      }
    }
    return multiIndexes.get(indexName);
  }

  @Override
  public IndexWriter registerIndex(MaterializedTable tableMeta, BasicColumn column)
      throws IOException {
    IndexWriter index;
    if (column.isUnique()) {
      index = registerUniqueIndex(tableMeta, column);
    } else {
      index = registerMultiIndex(tableMeta, column);
    }
    column.setIndexed(true);
    return index;
  }

  public void dropIndex(MaterializedTable tableMeta, BasicColumn column, AuthToken authToken)
      throws IOException, GrantException {
    grants.checkOwner(tableMeta, authToken);
    if (column.isUnique()) {
      dropUniqueIndex(tableMeta, column);
    } else {
      dropMultiIndex(tableMeta, column);
    }
    column.setIndexed(false);
  }

  private void dropUniqueIndex(MaterializedTable table, BasicColumn column) throws IOException {
    String indexName = table.tableName() + "." + column.getName();
    if (!uniqueIndexes.containsKey(indexName)) {
      return;
    }
    TableUniqueIndex index = uniqueIndexes.remove(indexName);
    index.drop();
  }

  private void dropMultiIndex(MaterializedTable table, BasicColumn column) throws IOException {
    String indexName = table.tableName() + "." + column.getName();
    if (!multiIndexes.containsKey(indexName)) {
      return;
    }
    TableMultiIndex index = multiIndexes.remove(indexName);
    index.drop();
  }

  public void createGrant(GrantToken grant, AuthToken authToken) throws GrantException {
    grants.createGrant(grant, authToken);
  }

  public void createUser(String username, String password, AuthToken authToken) throws GrantException {
    grants.createUser(username, password, authToken);
  }

  public void checkAccess(TableMetaResource resource, AuthToken authToken) throws GrantException {
    grants.checkAccess(resource, authToken);
  }

  public boolean hasAccess(MaterializedTable tableMeta, AuthToken authToken, Method method) {
    return grants.hasAccess(tableMeta, authToken, method);
  }

  public Resources resources(MetaResources metaResources) throws IOException {
    ImmutableMap.Builder<String, SeekableTableReader> readers = ImmutableMap.builder();
    ImmutableMap.Builder<String, TableWriter> writers = ImmutableMap.builder();
    ImmutableMap.Builder<String, MaterializedTable> metas = ImmutableMap.builder();
    for (TableMetaResource resource : metaResources.tables()) {
      if (resource.write()) {
        MaterializedTable tableMeta = resource.getTableMeta();
        writers.put(resource.getTableMeta().tableName(), new TableWriter(
            tableMeta,
            tableMeta.fileName(),
            tableMeta.fileWriter(),
            tableMeta.fileReader(),
            tableMeta.allColumns(),
            collectUniqueIndexes(tableMeta),
            collectMultiIndexes(tableMeta),
            resource.isForeignIndexes() ? collectForeignIndexes(tableMeta) : ImmutableMultimap.of(),
            resource.isReverseForeignIndexes() ? collectReverseForeignIndexes(tableMeta) : ImmutableMultimap.of(),
            ImmutableMap.copyOf(tableMeta.rules()),
            tableMeta.reverseRuleDependencies(),
            tableMeta.primaryKey()));
      } else {
        MaterializedTable tableMeta = resource.getTableMeta();
        readers.put(resource.getTableMeta().tableName(), new MaterializedTableReader(
            tableMeta,
            tableMeta.fileName(),
            tableMeta.fileReader(),
            tableMeta.allColumns(),
            collectIndexReaders(tableMeta)));
      }
      if (resource.isMeta()) {
        MaterializedTable tableMeta = resource.getTableMeta();
        metas.put(resource.getTableMeta().tableName(), tableMeta);
      }
    }
    return new Resources(readers.build(), writers.build(), metas.build());
  }

  public ImmutableMap<String, MaterializedTable> getTables(AuthToken authToken) {
    return ImmutableMap.copyOf(tables
        .entrySet()
        .stream()
        .filter(t -> grants.hasAccess(t.getValue(), authToken))
        .collect(Collectors.toList()));
  }

  public static class ModelException extends Exception {
    private static final long serialVersionUID = 1L;

    private final Loc loc;

    public ModelException(String msg, Ident ident) {
      super(msg);
      loc = Preconditions.checkNotNull(ident.getLoc());
    }

    public ModelException(String msg, Token token) {
      super(msg);
      loc = Preconditions.checkNotNull(token.getLoc());
    }

    public ModelException(String msg, Expression expr) {
      super(msg);
      loc = Preconditions.checkNotNull(expr.loc());
    }

    public ModelException(String msg, Loc loc) {
      super(msg);
      this.loc = Preconditions.checkNotNull(loc);
    }

    @Override
    public String getMessage() {
      return loc.toString() + ": " + super.getMessage();
    }

    public Loc getLoc() {
      return loc;
    }

    public String getSimpleMessage() {
      return super.getMessage();
    }
  }

  public static class RuleException extends Exception {
    private static final long serialVersionUID = 1L;

    public RuleException(String msg) {
      super(msg);
    }

    public RuleException(IndexException e) {
      super(e.getMessage());
    }
  }

  public void metaRepoReadLock() {
    lockManager.metaRepoReadLock();
  }

  public void metaRepoWriteLock() {
    lockManager.metaRepoWriteLock();
  }

  public void metaRepoReadUnlock() {
    lockManager.metaRepoReadUnlock();
  }

  public void metaRepoWriteUnlock() {
    lockManager.metaRepoWriteUnlock();
  }

  public boolean tryLock(MetaResources metaResources) {
    return lockManager.tryLock(metaResources);
  }

  public void unlock(MetaResources metaResources) {
    lockManager.unlock(metaResources);
  }

  public ImmutableMap<String, TableStat> tableStats() throws IOException {
    return Util.<String, MaterializedTable, TableStat>mapValuesIOException(tables, MaterializedTable::stat);
  }

  public ImmutableMap<String, ByteTrieStat> uniqueIndexStats() throws IOException {
    return Util.<String, TableUniqueIndex, ByteTrieStat>mapValuesIOException(uniqueIndexes, TableUniqueIndex::stats);
  }

  public ImmutableMap<String, ByteMultiTrieStat> multiIndexStats() throws IOException {
    return Util.<String, TableMultiIndex, ByteMultiTrieStat>mapValuesIOException(multiIndexes, TableMultiIndex::stats);
  }

  public IndexReader getIndex(String name) throws RuleException {
    if (uniqueIndexes.containsKey(name)) {
      return uniqueIndexes.get(name);
    } else if (multiIndexes.containsKey(name)) {
      return multiIndexes.get(name);
    } else {
      throw new RuleException(String.format("Invalid index '%s'.", name));
    }
  }

  @Override
  public int maxRefIndex() {
    return tables.values().stream().mapToInt(t -> t.refs().values().stream().mapToInt(r -> r.getIndex()).max().orElse(0)).max().orElse(0);
  }
}
