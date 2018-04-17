package com.cosyan.db.doc;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.StringJoiner;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.model.BuiltinFunctions;
import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableMap;

public class DocPrinter {

  public static void main(String[] args) throws IOException {
    String funcDocRootDir = args[0] + File.separator +
        "resources" + File.separator +
        "doc" + File.separator +
        "func" + File.separator;
    PrintWriter pw1 = new PrintWriter(funcDocRootDir + "simple_functions.md");
    try {
      for (SimpleFunction<?> function : BuiltinFunctions.SIMPLE) {
        printFunc(pw1, function);
      }
    } finally {
      pw1.close();
    }
    PrintWriter pw2 = new PrintWriter(funcDocRootDir + "aggregator_functions.md");
    try {
      for (AggrFunction function : BuiltinFunctions.AGGREGATIONS) {
        printFunc(pw2, function);
      }
    } finally {
      pw2.close();
    }
  }

  private static void printFunc(PrintWriter pw, SimpleFunction<?> function) {
    Func ann = function.getClass().getAnnotation(Func.class);
    if (ann == null) {
      throw new RuntimeException(function.getIdent());
    }
    ImmutableMap<String, DataType<?>> funcArgs = function.getArgTypes();
    StringBuilder sb = new StringBuilder();
    StringJoiner sj = new StringJoiner(", ");
    sb.append(" * `").append(function.getIdent()).append("(");
    for (Entry<String, DataType<?>> param : funcArgs.entrySet()) {
      sj.add(param.getKey() + ": " + param.getValue().getName());
    }
    sb.append(sj.toString());
    sb.append("): ").append(function.getReturnType().getName()).append("`<br/>\n");
    pw.print(sb.toString());
    String doc = ann.doc();
    for (String param : funcArgs.keySet()) {
      doc = doc.replaceAll("([ ,.])" + param + "([ ,.])", "$1`" + param + "`$2");
    }
    pw.append("   " + doc + "\n");
  }

  private static void printFunc(PrintWriter pw, AggrFunction function) {
    Func ann = function.getClass().getAnnotation(Func.class);
    if (ann == null) {
      throw new RuntimeException(function.getIdent());
    }
    StringBuilder sb = new StringBuilder();
    sb.append(" * `").append(function.getIdent()).append("(arg)").append("`<br/>\n");
    pw.print(sb.toString());
    String doc = ann.doc();
    pw.append("   " + doc + "\n");
  }
}
