package com.cosyan.db.doc;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.TreeMap;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.doc.FunctionDocumentation.FuncCat;
import com.cosyan.db.model.Aggregators;
import com.cosyan.db.model.BuiltinFunctions;
import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.DateFunctions;
import com.cosyan.db.model.ListAggregators;
import com.cosyan.db.model.MathFunctions;
import com.cosyan.db.model.StatAggregators;
import com.cosyan.db.model.StringFunctions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class DocPrinter {

  public static void main(String[] args) throws IOException {
    String funcDocRootDir = args[0] + File.separator +
        "resources" + File.separator +
        "doc" + File.separator +
        "func" + File.separator;
    PrintWriter pw1 = new PrintWriter(funcDocRootDir + "simple_functions.md");
    try {
      ImmutableList<Class<?>> categories = ImmutableList.of(
          StringFunctions.class,
          MathFunctions.class,
          DateFunctions.class);
      Map<Class<?>, Map<String, String>> docss = new LinkedHashMap<>();
      for (Class<?> clss : categories) {
        docss.put(clss, new TreeMap<>());
      }
      for (SimpleFunction<?> function : BuiltinFunctions.SIMPLE) {
        Map<String, String> docs = docss.get(function.getClass().getEnclosingClass());
        printFunc(function, docs);
      }
      for (Class<?> clss : categories) {
        FuncCat funcCat = clss.getAnnotation(FuncCat.class);
        pw1.print("## " + funcCat.doc() + "\n\n");
        for (String doc : docss.get(clss).values()) {
          pw1.print(doc);
        }
      }
    } finally {
      pw1.close();
    }

    PrintWriter pw2 = new PrintWriter(funcDocRootDir + "aggregator_functions.md");
    try {
      ImmutableList<Class<?>> categories = ImmutableList.of(
          Aggregators.class,
          StatAggregators.class,
          ListAggregators.class);
      Map<Class<?>, Map<String, String>> docss = new LinkedHashMap<>();
      for (Class<?> clss : categories) {
        docss.put(clss, new TreeMap<>());
      }
      for (AggrFunction function : BuiltinFunctions.AGGREGATIONS) {
        Map<String, String> docs = docss.get(function.getClass().getEnclosingClass());
        printFunc(function, docs);
      }
      for (Class<?> clss : categories) {
        FuncCat funcCat = clss.getAnnotation(FuncCat.class);
        pw2.print("## " + funcCat.doc() + "\n\n");
        for (String doc : docss.get(clss).values()) {
          pw2.print(doc);
        }
      }
    } finally {
      pw2.close();
    }
  }

  private static void printFunc(SimpleFunction<?> function, Map<String, String> funcMap) {
    Func ann = function.getClass().getAnnotation(Func.class);
    FuncCat funcCat = function.getClass().getEnclosingClass().getAnnotation(FuncCat.class);
    if (ann == null || funcCat == null) {
      throw new RuntimeException(function.getName());
    }
    ImmutableMap<String, DataType<?>> funcArgs = function.getArgTypes();
    StringBuilder sb = new StringBuilder();
    StringJoiner sj = new StringJoiner(", ");
    sb.append(" * `").append(function.getName()).append("(");
    for (Entry<String, DataType<?>> param : funcArgs.entrySet()) {
      sj.add(param.getKey() + ": " + param.getValue().getName());
    }
    sb.append(sj.toString());
    sb.append("): ").append(function.getReturnType().getName()).append("`<br/>\n");
    String doc = ann.doc();
    for (String param : funcArgs.keySet()) {
      doc = doc.replaceAll("([ ,.])" + param + "([ ,.])", "$1`" + param + "`$2");
    }
    sb.append("   " + doc + "\n\n");
    funcMap.put(function.getName(), sb.toString());
  }

  private static void printFunc(AggrFunction function, Map<String, String> funcMap) {
    Func ann = function.getClass().getAnnotation(Func.class);
    FuncCat funcCat = function.getClass().getEnclosingClass().getAnnotation(FuncCat.class);
    if (ann == null || funcCat == null) {
      throw new RuntimeException(function.getName());
    }
    StringBuilder sb = new StringBuilder();
    sb.append(" * `").append(function.getName()).append("(arg)").append("`<br/>\n");
    String doc = ann.doc();
    sb.append("   " + doc + "\n\n");
    funcMap.put(function.getName(), sb.toString());
  }
}
