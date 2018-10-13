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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.cosyan.db.conf.Config;
import com.cosyan.db.lang.expr.TableDefinition.ForeignKeyDefinition;
import com.cosyan.db.lang.expr.TableDefinition.RuleDefinition;
import com.cosyan.db.lang.expr.TableDefinition.TableWithOwnerDefinition;
import com.cosyan.db.lang.expr.TableDefinition.ViewDefinition;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableRef;
import com.cosyan.db.session.ILexer;
import com.cosyan.db.session.IParser;
import com.cosyan.db.session.IParser.ParserException;
import com.google.common.collect.ImmutableMap;

public class MetaSerializer {

  private final ILexer lexer;
  private final IParser parser;

  public MetaSerializer(ILexer lexer, IParser parser) {
    this.lexer = lexer;
    this.parser = parser;
  }

  public JSONObject toJSON(MaterializedTable table) {
    JSONObject obj = new JSONObject();
    obj.put("name", table.tableName());
    obj.put("owner", table.owner());
    obj.put("type", table.type().name());
    if (table.primaryKey().isPresent()) {
      obj.put("primary_key", new JSONObject(ImmutableMap.of(
          "name", table.primaryKey().get().getName().getString(),
          "column", table.primaryKey().get().getColumn().getName())));
    }
    obj.put("columns", table.allColumns().stream().map(c -> toJSON(c)).collect(Collectors.toList()));
    obj.put("foreign_keys", table.foreignKeys().values().stream().map(fk -> toJSON(fk)).collect(Collectors.toList()));
    obj.put("refs", table.refs().values().stream().map(r -> toJSON(r)).collect(Collectors.toList()));
    obj.put("rules", table.rules().values().stream().map(r -> toJSON(r)).collect(Collectors.toList()));
    return obj;
  }

  public JSONObject toJSON(BasicColumn column) {
    JSONObject obj = new JSONObject();
    obj.put("name", column.getName());
    obj.put("type", column.getType().toJSON());
    obj.put("unique", column.isUnique());
    obj.put("indexed", column.isIndexed());
    obj.put("nullable", column.isNullable());
    obj.put("immutable", column.isImmutable());
    obj.put("deleted", column.isDeleted());
    return obj;
  }

  public JSONObject toJSON(ForeignKey foreignKey) {
    JSONObject obj = new JSONObject();
    obj.put("name", foreignKey.getName());
    obj.put("rev_name", foreignKey.getRevName());
    obj.put("column", foreignKey.getColumn().getName());
    obj.put("ref_table_owner", foreignKey.getRefTable().owner());
    obj.put("ref_table_name", foreignKey.getRefTable().tableName());
    return obj;
  }

  public JSONObject toJSON(TableRef ref) {
    JSONObject obj = new JSONObject();
    obj.put("name", ref.getName());
    obj.put("expr", ref.getExpr() + ";");
    obj.put("index", ref.getIndex());
    return obj;
  }

  public JSONObject toJSON(Rule rule) {
    JSONObject obj = new JSONObject();
    obj.put("name", rule.getName());
    obj.put("null_is_true", rule.isNullIsTrue());
    obj.put("expr", rule.getExpr().print());
    return obj;
  }

  public Map<String, Map<String, MaterializedTable>> loadTables(Config config, List<JSONObject> jsons)
      throws JSONException, IOException, ModelException, ParserException {
    Map<String, Map<String, MaterializedTable>> tables = new HashMap<>();
    for (JSONObject json : jsons) {
      String name = json.getString("name");
      MaterializedTable table = table(config, name, json);
      if (!tables.containsKey(table.owner())) {
        tables.put(table.owner(), new HashMap<>());
      }
      tables.get(table.owner()).put(name, table);
    }

    for (JSONObject json : jsons) {
      loadForeignKeys(tables.get(json.get("owner")).get(json.get("name")), json, tables);
    }
    for (MaterializedTable table : MetaRepo.allTables(tables)) {
      for (ForeignKey foreignKey : table.foreignKeys().values()) {
        foreignKey.getRefTable().addReverseForeignKey(foreignKey.createReverse());
      }
    }
    TreeMap<Integer, Pair<MaterializedTable, JSONObject>> refObjects = new TreeMap<>();
    for (JSONObject json : jsons) {
      collectRefs(tables.get(json.get("owner")).get(json.get("name")), json, refObjects);
    }
    for (Pair<MaterializedTable, JSONObject> pair : refObjects.values()) {
      loadRef(pair.getLeft(), pair.getRight());
    }
    for (JSONObject json : jsons) {
      loadRules(tables.get(json.get("owner")).get(json.get("name")), json);
    }
    for (MaterializedTable table : MetaRepo.allTables(tables)) {
      for (BooleanRule rule : table.rules().values()) {
        rule.getDeps().forAllReverseRuleDependencies(rule, /* add= */true);
      }
    }
    return tables;
  }

  public void loadForeignKeys(MaterializedTable table, JSONObject obj, Map<String, Map<String, MaterializedTable>> tables)
      throws JSONException, IOException, ModelException {
    JSONArray arr = obj.getJSONArray("foreign_keys");
    for (int i = 0; i < arr.length(); i++) {
      JSONObject fkObj = arr.getJSONObject(i);
      ForeignKeyDefinition fkDef = new ForeignKeyDefinition(
          new Ident(fkObj.getString("name")),
          new Ident(fkObj.getString("rev_name")),
          new Ident(fkObj.getString("column")),
          new TableWithOwnerDefinition(
              Optional.of(new Ident(fkObj.getString("ref_table_owner"))),
              new Ident(fkObj.getString("ref_table_name"))),
          Optional.empty());
      MaterializedTable refTable = tables.get(fkObj.getString("ref_table_owner")).get(fkObj.getString("ref_table_name"));
      ForeignKey fk = table.createForeignKey(fkDef, refTable);
      fk.getColumn().setIndexed(true);
      table.addForeignKey(fk);
    }
  }

  private void collectRefs(MaterializedTable table, JSONObject obj,
      TreeMap<Integer, Pair<MaterializedTable, JSONObject>> refObjects)
      throws JSONException, IOException, ModelException, ParserException {
    JSONArray arr = obj.getJSONArray("refs");
    for (int i = 0; i < arr.length(); i++) {
      JSONObject refObj = arr.getJSONObject(i);
      refObjects.put(refObj.getInt("index"), ImmutablePair.of(table, refObj));
    }
  }

  public void loadRef(MaterializedTable table, JSONObject refObj)
      throws JSONException, IOException, ModelException, ParserException {
    String name = refObj.getString("name");
    String expr = refObj.getString("expr");
    TableMeta refTableMeta;
    ViewDefinition ref = new ViewDefinition(
        new Ident(name),
        parser.parseSelect(lexer.tokenizeExpression(expr)));
    refTableMeta = table.createView(ref, table.owner());
    table.addRef(new TableRef(name, expr, refObj.getInt("index"), refTableMeta));
  }

  public void loadRules(MaterializedTable table, JSONObject obj)
      throws JSONException, IOException, ModelException, ParserException {
    JSONArray arr = obj.getJSONArray("rules");
    for (int i = 0; i < arr.length(); i++) {
      JSONObject ruleObj = arr.getJSONObject(i);
      RuleDefinition ruleDefinition = new RuleDefinition(
          new Ident(ruleObj.getString("name")),
          parser.parseExpression(lexer.tokenizeExpression(ruleObj.getString("expr"))),
          ruleObj.getBoolean("null_is_true"));
      BooleanRule rule = table.createRule(ruleDefinition);
      table.addRule(rule);
    }
  }

  public MaterializedTable table(Config config, String tableName, JSONObject obj)
      throws JSONException, IOException, ModelException {
    List<BasicColumn> columns = columns(obj.getJSONArray("columns"));
    Optional<PrimaryKey> pk = Optional.empty();
    if (obj.has("primary_key")) {
      JSONObject pkObj = obj.getJSONObject("primary_key");
      pk = Optional.of(new PrimaryKey(
          new Ident(pkObj.getString("name")),
          columns.stream().filter(c -> c.getName().equals(pkObj.getString("column"))).findFirst().get()));
    }
    return new MaterializedTable(
        config,
        tableName,
        obj.getString("owner"),
        columns,
        pk,
        MaterializedTable.Type.valueOf(obj.getString("type")));
  }

  public List<BasicColumn> columns(JSONArray arr) throws JSONException, ModelException {
    ArrayList<BasicColumn> columns = new ArrayList<>();
    for (int i = 0; i < arr.length(); i++) {
      JSONObject colObj = arr.getJSONObject(i);
      BasicColumn column = new BasicColumn(
          i,
          new Ident(colObj.getString("name")),
          DataTypes.fromJSON(colObj.getJSONObject("type")),
          colObj.getBoolean("nullable"),
          colObj.getBoolean("unique"),
          colObj.getBoolean("immutable"));
      column.setDeleted(colObj.getBoolean("deleted"));
      column.setIndexed(colObj.getBoolean("indexed"));
      columns.add(column);
    }
    return columns;
  }
}
