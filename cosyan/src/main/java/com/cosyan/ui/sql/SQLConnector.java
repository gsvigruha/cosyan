package com.cosyan.ui.sql;

import java.io.IOException;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.sql.Compiler;
import com.cosyan.db.sql.Parser;
import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.SyntaxTree;
import com.google.common.collect.ImmutableMap;

public class SQLConnector {

  private final Parser parser;
  private final Compiler compiler;

  public SQLConnector(Compiler compiler) {
    this.compiler = compiler;
    this.parser = new Parser();
  }

  @SuppressWarnings("unchecked")
  public JSONObject run(String sql)
      throws ModelException, ConfigException, ParserException, IOException, IndexException {
    SyntaxTree tree = parser.parse(sql);
    JSONObject obj = new JSONObject();
    if (tree.isSelect()) {
      ExposedTableReader reader = compiler.query(tree).reader();
      JSONArray list = new JSONArray();
      ImmutableMap<String, Object> row = reader.readColumns();
      obj.put("columns", row.keySet().asList());
      while (row != null) {
        JSONObject rowObj = new JSONObject();
        for (Entry<String, Object> value : row.entrySet()) {
          rowObj.put(value.getKey(), value.getValue());
        }
        list.add(rowObj);
        row = reader.readColumns();
      }
      obj.put("result", list);
    } else if (tree.isStatement()) {
      try {
        boolean success = compiler.statement(tree);
        obj.put("result", success);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return obj;
  }
}
