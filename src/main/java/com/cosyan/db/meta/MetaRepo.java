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
package com.cosyan.db.meta;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.conf.Config;
import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.IndexStat.ByteMultiTrieStat;
import com.cosyan.db.index.IndexStat.ByteTrieStat;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.MetaSerializer;
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
import com.cosyan.db.meta.TableProvider.TableWithOwner;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.Rule.BooleanViewRule;
import com.cosyan.db.model.SeekableTableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMultiIndex;
import com.cosyan.db.model.TableUniqueIndex;
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

public class MetaRepo {

  private final Config config;
  private final HashMap<String, Map<String, MaterializedTable>> tables;
  private final HashMap<String, Map<String, View>> views;
  private final Grants grants;

  private final LockManager lockManager;
  private final MetaSerializer metaSerializer;

  public MetaRepo(
      Config config,
      LockManager lockManager,
      Grants grants,
      ILexer lexer,
      IParser parser)
      throws IOException, DBException {
    this.config = config;
    this.lockManager = lockManager;
    this.metaSerializer = new MetaSerializer(lexer, parser);
    this.tables = new HashMap<>();
    this.views = new HashMap<>();
    this.grants = grants;

    Files.createDirectories(Paths.get(config.tableDir()));
    Files.createDirectories(Paths.get(config.indexDir()));
    Files.createDirectories(Paths.get(config.journalDir()));
    Files.createDirectories(Paths.get(config.metaDir()));

    readTables();
  }

  public Config config() {
    return config;
  }

  public static List<MaterializedTable> allTables(Map<String, Map<String, MaterializedTable>> tables) {
    return tables.values().stream().flatMap(m -> m.values().stream()).collect(Collectors.toList());
  }

  private List<MaterializedTable> allTables() {
    return allTables(tables);
  }

  private Map<String, MaterializedTable> tablesWithNames() {
    return allTables().stream().collect(Collectors.toMap(t -> t.owner() + "." + t.name(), t -> t));
  }

  public void init() throws IOException {
    for (MaterializedTable tableMeta : allTables()) {
      tableMeta.loadStats();
    }
  }

  public void shutdown() throws IOException {
    for (MaterializedTable tableMeta : allTables()) {
      tableMeta.saveStats();
    }
  }

  public void writeTables() throws IOException {
    for (MaterializedTable table : allTables()) {
      JSONObject obj = metaSerializer.toJSON(table);
      FileUtils.writeStringToFile(
          new File(config.metaDir() + File.separator + table.fullName()),
          obj.toString(),
          Charset.defaultCharset());
    }
    FileUtils.writeStringToFile(
        new File(config.usersFile()),
        grants.toJSON().toString(),
        Charset.defaultCharset());
  }

  public void readTables() throws DBException {
    Map<String, Map<String, MaterializedTable>> newTables;
    try {
      File file = new File(config.metaDir());
      List<JSONObject> jsons = new ArrayList<>();
      for (String fileName : file.list()) {
        JSONObject json = new JSONObject(FileUtils.readFileToString(
            new File(config.metaDir() + File.separator + fileName),
            Charset.defaultCharset()));
        jsons.add(json);
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
    lockManager.syncLocks(allTables());
    try {
      for (MaterializedTable table : allTables()) {
        table.syncIndex();
      }
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  @VisibleForTesting
  public MaterializedTable table(String owner, String name) throws ModelException {
    assert tables.containsKey(owner) : String.format("User '%s' does not exist.", owner);
    assert tables.get(owner).containsKey(name) : String.format("Table '%s.%s' does not exist.", owner, name);
    return tables.get(owner).get(name);
  }

  @VisibleForTesting
  public View view(String owner, String name) throws ModelException {
    assert views.containsKey(owner) : String.format("User '%s' does not exist.", owner);
    assert views.get(owner).containsKey(name) : String.format("View '%s.%s' does not exist.", owner, name);
    return views.get(owner).get(name);
  }

  public TableProvider tableProvider(Ident ident, String owner) throws ModelException {
    if (tables.containsKey(ident.getString())) {
      // Check first is ident is an existing owner.
      Map<String, MaterializedTable> userTables = tables.get(ident.getString());
      return new TableProvider() {
        private SeekableTableMeta reader(Ident ident2) throws ModelException {
          if (!userTables.containsKey(ident2.getString())) {
            throw new ModelException(String.format("Table '%s.%s' does not exist.", ident, ident2), ident2);
          }
          return userTables.get(ident2.getString()).reader();
        }

        @Override
        public TableProvider tableProvider(Ident ident2, String owner) throws ModelException {
          return reader(ident2);
        }

        @Override
        public ExposedTableMeta tableMeta(TableWithOwner table) throws ModelException {
          return reader(table.getTable());
        }
      };
    } else if (tables.containsKey(owner)) {
      // Otherwise check if it matches a table owned by the current user (owner).
      Map<String, MaterializedTable> userTables = tables.get(owner);
      if (userTables.containsKey(ident.getString())) {
        return userTables.get(ident.getString()).reader();
      }
    }
    throw new ModelException(String.format("Table '%s' does not exist.", ident), ident);
  }

  @VisibleForTesting
  public boolean hasTable(String tableName, String owner) {
    if (!tables.containsKey(owner)) {
      return false;
    }
    return tables.get(owner).containsKey(tableName);
  }

  private List<MaterializedTable> getTables(AuthToken authToken) {
    return allTables()
        .stream()
        .filter(t -> grants.hasAccess(t, authToken))
        .collect(Collectors.toList());
  }

  public ImmutableMultimap<String, IndexReader> collectForeignIndexes(MaterializedTable table) {
    ImmutableMultimap.Builder<String, IndexReader> builder = ImmutableMultimap.builder();
    for (ForeignKey foreignKey : table.foreignKeys().values()) {
      builder.put(
          foreignKey.getColumn().getName(),
          foreignKey.getRefTable().uniqueIndexes().get(foreignKey.getRefColumn().getName()));
    }
    return builder.build();
  }

  public ImmutableMultimap<String, IndexReader> collectReverseForeignIndexes(MaterializedTable table) {
    ImmutableMultimap.Builder<String, IndexReader> builder = ImmutableMultimap.builder();
    for (ReverseForeignKey reverseForeignKey : table.reverseForeignKeys().values()) {
      if (reverseForeignKey.getRefColumn().isUnique()) {
        builder.put(
            reverseForeignKey.getColumn().getName(),
            reverseForeignKey.getRefTable().uniqueIndexes().get(reverseForeignKey.getRefColumn().getName()));
      } else {
        builder.put(
            reverseForeignKey.getColumn().getName(),
            reverseForeignKey.getRefTable().multiIndexes().get(reverseForeignKey.getRefColumn().getName()));
      }
    }
    return builder.build();
  }

  private void checkAccess(TableMetaResource resource, AuthToken authToken) throws GrantException {
    grants.checkAccess(resource, authToken);
  }

  @Nullable
  private MaterializedTable tableOrNull(TableWithOwner table) {
    return Optional.ofNullable(tables.get(table.getOwner()))
        .map(t -> Optional.ofNullable(t.get(table.getTable().getString()))).orElse(Optional.empty()).orElse(null);
  }

  @Nullable
  private View viewOrNull(TableWithOwner table) {
    return Optional.ofNullable(views.get(table.getOwner()))
        .map(t -> Optional.ofNullable(t.get(table.getTable().getString()))).orElse(Optional.empty()).orElse(null);
  }

  private MaterializedTable table(TableWithOwner table) throws ModelException {
    MaterializedTable tableMeta = tableOrNull(table);
    if (tableMeta == null) {
      throw new ModelException(String.format("Table '%s' does not exist.", table), table.getTable());
    }
    return tableMeta;
  }

  private View view(TableWithOwner table) throws ModelException {
    View view = viewOrNull(table);
    if (view == null) {
      throw new ModelException(String.format("View '%s' does not exist.", table), table.getTable());
    }
    return view;
  }

  private ExposedTableMeta tableOrView(TableWithOwner ident) throws ModelException {
    MaterializedTable table = tableOrNull(ident);
    if (table != null) {
      return table.reader();
    }
    View view = viewOrNull(ident);
    if (view != null) {
      return view.table();
    }
    throw new ModelException(String.format("Table or view '%s' does not exist.", ident), ident.getTable());
  }

  public Resources resources(MetaResources metaResources, AuthToken authToken) throws IOException {
    ImmutableMap.Builder<String, SeekableTableReader> readers = ImmutableMap.builder();
    ImmutableMap.Builder<String, TableWriter> writers = ImmutableMap.builder();
    ImmutableMap.Builder<String, DBObject> metas = ImmutableMap.builder();
    for (TableMetaResource resource : metaResources.tables()) {
      if (resource.write()) {
        MaterializedTable tableMeta = (MaterializedTable) resource.getObject();
        writers.put(tableMeta.fullName(), new TableWriter(
            tableMeta,
            tableMeta.fileName(),
            tableMeta.fileWriter(),
            tableMeta.fileReader(),
            tableMeta.allColumns(),
            tableMeta.uniqueIndexes(),
            tableMeta.multiIndexes(),
            tableMeta.extraIndexes(),
            resource.isForeignIndexes() ? collectForeignIndexes(tableMeta) : ImmutableMultimap.of(),
            resource.isReverseForeignIndexes() ? collectReverseForeignIndexes(tableMeta) : ImmutableMultimap.of(),
            ImmutableMap.copyOf(tableMeta.rules()),
            tableMeta.reverseRuleDependencies(),
            tableMeta.primaryKey()));
      } else {
        DBObject object = resource.getObject();
        if (object instanceof MaterializedTable) {
          readers.put(object.fullName(), ((MaterializedTable) object).createReader());
        }
      }
      if (resource.isMeta()) {
        DBObject meta = resource.getObject();
        metas.put(meta.fullName(), meta);
      }
    }
    return new Resources(readers.build(), writers.build(), metas.build());
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

  public MetaReader metaRepoReadLock() {
    lockManager.metaRepoReadLock();
    return new MetaReader() {

      @Override
      public MaterializedTable table(TableWithOwner table) throws ModelException {
        return MetaRepo.this.table(table);
      }

      @Override
      public ExposedTableMeta tableMeta(TableWithOwner table) throws ModelException {
        return MetaRepo.this.tableOrView(table);
      }

      @Override
      public TableProvider tableProvider(Ident ident, String owner) throws ModelException {
        return MetaRepo.this.tableProvider(ident, owner);
      }

      @Override
      public ImmutableMap<String, TableStat> tableStats() throws IOException {
        return Util.<String, MaterializedTable, TableStat>mapValuesIOException(tablesWithNames(),
            MaterializedTable::stat);
      }

      @Override
      public ImmutableMap<String, ByteTrieStat> uniqueIndexStats() throws IOException {
        ImmutableMap.Builder<String, ByteTrieStat> builder = ImmutableMap.builder();
        for (MaterializedTable table : allTables()) {
          for (Map.Entry<String, TableUniqueIndex> index : table.uniqueIndexes().entrySet()) {
            builder.put(table.fullName() + "." + index.getKey(), index.getValue().stats());
          }
        }
        return builder.build();
      }

      @Override
      public ImmutableMap<String, ByteMultiTrieStat> multiIndexStats() throws IOException {
        ImmutableMap.Builder<String, ByteMultiTrieStat> builder = ImmutableMap.builder();
        for (MaterializedTable table : allTables()) {
          for (Map.Entry<String, TableMultiIndex> index : table.multiIndexes().entrySet()) {
            builder.put(table.fullName() + "." + index.getKey(), index.getValue().stats());
          }
        }
        return builder.build();
      }

      @Override
      public JSONArray collectUsers() {
        return grants.toJSON();
      }

      @Override
      public List<MaterializedTable> getTables(AuthToken authToken) {
        return MetaRepo.this.getTables(authToken);
      }

      @Override
      public void checkAccess(TableMetaResource resource, AuthToken authToken) throws GrantException {
        MetaRepo.this.checkAccess(resource, authToken);
      }

      @Override
      public void metaRepoReadUnlock() {
        lockManager.metaRepoReadUnlock();
      }

      @Override
      public IndexReader getIndex(String id) throws RuleException {
        String[] ids = id.split("\\.");
        return tables.get(ids[0]).get(ids[1]).getIndex(ids[2]);
      }
    };
  }

  public MetaWriter metaRepoWriteLock() {
    lockManager.metaRepoWriteLock();
    return new MetaWriter() {

      @Override
      public MaterializedTable table(TableWithOwner ident, AuthToken authToken) throws ModelException, GrantException {
        MaterializedTable tableMeta = MetaRepo.this.table(ident);
        grants.checkOwner(tableMeta, authToken);
        return tableMeta;
      }

      @Override
      public View view(TableWithOwner ident, AuthToken authToken) throws ModelException, GrantException {
        View view = MetaRepo.this.view(ident);
        grants.checkOwner(view, authToken);
        return view;
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

      @Override
      public void syncMeta(View view) {
        for (BooleanViewRule rule : view.rules().values()) {
          rule.getDeps().forAllReverseRuleDependencies(rule, /* add= */true);
        }
      }

      @Override
      public void registerTable(MaterializedTable tableMeta) throws IOException {
        tableMeta.syncIndex();
        syncMeta(tableMeta);
        if (!tables.containsKey(tableMeta.owner())) {
          tables.put(tableMeta.owner(), new HashMap<>());
        }
        tables.get(tableMeta.owner()).put(tableMeta.name(), tableMeta);
        lockManager.registerLock(tableMeta);
      }

      @Override
      public void registerView(View view) {
        syncMeta(view);
        if (!views.containsKey(view.owner())) {
          views.put(view.owner(), new HashMap<>());
        }
        views.get(view.owner()).put(view.name(), view);
        lockManager.registerLock(view);
      }

      @Override
      public void dropTable(MaterializedTable tableMeta, AuthToken authToken) throws IOException, GrantException {
        grants.checkOwner(tableMeta, authToken);
        tables.get(tableMeta.owner()).remove(tableMeta.name());
        tableMeta.drop();
        lockManager.removeLock(tableMeta);
      }

      @Override
      public int maxRefIndex() {
        return allTables().stream()
            .mapToInt(t -> t.refs().values().stream().mapToInt(r -> r.getIndex()).max().orElse(0)).max().orElse(0);
      }

      @Override
      public Config config() {
        return config;
      }

      @Override
      public boolean hasTable(String tableName, String owner) {
        return MetaRepo.this.hasTable(tableName, owner);
      }

      @Override
      public List<MaterializedTable> getTables(AuthToken authToken) {
        return MetaRepo.this.getTables(authToken);
      }

      @Override
      public void createGrant(GrantToken grant, AuthToken authToken) throws GrantException {
        grants.createGrant(grant, authToken);
      }

      @Override
      public void createUser(String username, String password, AuthToken authToken) throws GrantException {
        grants.createUser(username, password, authToken);
      }

      @Override
      public void checkAccess(TableMetaResource resource, AuthToken authToken) throws GrantException {
        MetaRepo.this.checkAccess(resource, authToken);
      }

      @Override
      public boolean hasAccess(MaterializedTable tableMeta, AuthToken authToken, Method method) {
        return grants.hasAccess(tableMeta, authToken, method);
      }

      @Override
      public void resetAndReadTables() throws DBException {
        readTables();
      }

      @Override
      public void metaRepoWriteUnlock() {
        lockManager.metaRepoWriteUnlock();
      }

      @Override
      public ExposedTableMeta tableMeta(TableWithOwner table) throws ModelException {
        return MetaRepo.this.table(table).reader();
      }

      @Override
      public TableProvider tableProvider(Ident ident, String owner) throws ModelException {
        return MetaRepo.this.tableProvider(ident, owner);
      }
    };
  }

  public boolean tryLock(MetaResources metaResources) {
    return lockManager.tryLock(metaResources);
  }

  public void unlock(MetaResources metaResources) {
    lockManager.unlock(metaResources);
  }
}
