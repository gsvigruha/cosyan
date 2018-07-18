package com.cosyan.db.doc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.commonmark.node.Node;
import org.commonmark.node.Text;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;
import org.json.JSONArray;
import org.json.JSONObject;

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
    String resourcesDir = args[0] + File.separator + "resources";
    String funcDocRootDir = resourcesDir + File.separator + "doc" + File.separator + "func"
        + File.separator;
    printSimpleFunctions(funcDocRootDir);

    printAggrFunctions(funcDocRootDir);

    markdownToHtml(resourcesDir);
  }

  private static void markdownToHtml(String resourcesDir) throws IOException {
    File docRoot = new File(resourcesDir + File.separator + "doc");
    Collection<File> files = FileUtils
        .listFiles(docRoot, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).stream()
        .sorted((f1, f2) -> f1.getName().compareTo(f2.getName())).collect(Collectors.toList());
    JSONArray items = new JSONArray();
    String webRoot = "web" + File.separator + "app" + File.separator + "help";

    for (File markdown : files) {
      Parser parser = Parser.builder().build();
      Node document = parser.parse(FileUtils.readFileToString(markdown, Charset.defaultCharset()));
      HtmlRenderer renderer = HtmlRenderer.builder().build();
      String suffix = markdown.getAbsolutePath().substring(docRoot.getAbsolutePath().length() + 1,
          markdown.getAbsolutePath().length() - 3);
      File html = new File(webRoot + File.separator + suffix + ".html");
      FileUtils.writeStringToFile(html, renderer.render(document), Charset.defaultCharset());
      JSONObject object = new JSONObject();
      object.put("url", suffix);
      object.put("title", ((Text) document.getFirstChild().getFirstChild()).getLiteral());
      items.put(object);
    }
    FileUtils.writeStringToFile(new File(webRoot + File.separator + "list"), items.toString(),
        Charset.defaultCharset());
  }

  private static void printAggrFunctions(String funcDocRootDir) throws FileNotFoundException {
    ImmutableList<Class<?>> categories = ImmutableList.of(Aggregators.class, StatAggregators.class,
        ListAggregators.class);
    Map<Class<?>, Map<String, String>> docss = new LinkedHashMap<>();
    for (Class<?> clss : categories) {
      docss.put(clss, new TreeMap<>());
    }
    for (AggrFunction function : BuiltinFunctions.AGGREGATIONS) {
      Map<String, String> docs = docss.get(function.getClass().getEnclosingClass());
      printFunc(function, docs);
    }
    int i = 1;
    for (Class<?> clss : categories) {
      FuncCat funcCat = clss.getAnnotation(FuncCat.class);
      PrintWriter aggrFunctionsPrinter = new PrintWriter(
          funcDocRootDir + "2" + i + "_" + funcCat.doc().toLowerCase().replace(" ", "_") + ".md");
      try {
        aggrFunctionsPrinter.print("### " + funcCat.doc() + "\n\n");
        for (String doc : docss.get(clss).values()) {
          aggrFunctionsPrinter.print(doc);
        }
      } finally {
        aggrFunctionsPrinter.close();
      }
      i++;
    }
  }

  private static void printSimpleFunctions(String funcDocRootDir) throws FileNotFoundException {
    ImmutableList<Class<?>> categories = ImmutableList.of(StringFunctions.class,
        MathFunctions.class, DateFunctions.class);
    Map<Class<?>, Map<String, String>> docss = new LinkedHashMap<>();
    for (Class<?> clss : categories) {
      docss.put(clss, new TreeMap<>());
    }
    for (SimpleFunction<?> function : BuiltinFunctions.SIMPLE) {
      Map<String, String> docs = docss.get(function.getClass().getEnclosingClass());
      printFunc(function, docs);
    }
    int i = 1;
    for (Class<?> clss : categories) {
      FuncCat funcCat = clss.getAnnotation(FuncCat.class);
      PrintWriter simpleFunctionsPrinter = new PrintWriter(
          funcDocRootDir + "1" + i + "_" + funcCat.doc().toLowerCase().replace(" ", "_") + ".md");
      try {
        simpleFunctionsPrinter.print("### " + funcCat.doc() + "\n\n");
        for (String doc : docss.get(clss).values()) {
          simpleFunctionsPrinter.print(doc);
        }
      } finally {
        simpleFunctionsPrinter.close();
      }
      i++;
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
