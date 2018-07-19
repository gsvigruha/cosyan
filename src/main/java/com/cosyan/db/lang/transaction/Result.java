package com.cosyan.db.lang.transaction;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.DateFunctions;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public abstract class Result {

  private final boolean success;

  public abstract JSONObject toJSON();

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class QueryResult extends Result {

    private final ImmutableList<String> header;
    private final ImmutableList<DataType<?>> types;
    private final ImmutableList<Object[]> values;

    public QueryResult(ImmutableList<String> header, ImmutableList<DataType<?>> types,
        Iterable<Object[]> values) {
      super(true);
      this.header = header;
      this.types = types;
      this.values = ImmutableList.copyOf(values);
    }

    private List<List<String>> listValues() {
      return values.stream().map(l -> prettyPrintToList(l, types)).collect(Collectors.toList());
    }

    public static String prettyPrint(Object obj, DataType<?> type) {
      if (obj == null) {
        return "null";
      } else {
        return type.toString(obj);
      }
    }

    public static String prettyPrint(Object[] values, ImmutableList<DataType<?>> types) {
      StringJoiner vsj = new StringJoiner(",");
      for (int i = 0; i < values.length; i++) {
        vsj.add(prettyPrint(values[i], types.get(i)));
      }
      return vsj.toString() + "\n";
    }

    public static List<String> prettyPrintToList(Object[] values,
        ImmutableList<DataType<?>> types) {
      ArrayList<String> result = new ArrayList<>();
      for (int i = 0; i < values.length; i++) {
        result.add(prettyPrint(values[i], types.get(i)));
      }
      return result;
    }

    public static String prettyPrintHeader(Iterable<String> header) {
      StringJoiner sj = new StringJoiner(",");
      for (String col : header) {
        sj.add(col);
      }
      return sj.toString() + "\n";
    }

    public String prettyPrint() {
      StringBuilder sb = new StringBuilder();
      sb.append(prettyPrintHeader(header));
      for (Object[] row : values) {
        sb.append(prettyPrint(row, types));
      }
      return sb.toString();
    }

    @Override
    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("type", "query");
      obj.put("header", getHeader());
      obj.put("types", types.stream().map(t -> t.toJSON()).collect(Collectors.toList()));
      obj.put("values", listValues());
      return obj;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class StatementResult extends Result {

    private final long affectedLines;

    public StatementResult(long affectedLines) {
      super(true);
      this.affectedLines = affectedLines;
    }

    @Override
    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("type", "statement");
      obj.put("lines", getAffectedLines());
      obj.put("msg", String.format("Statement affected %s lines.", getAffectedLines()));
      return obj;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class InsertIntoResult extends StatementResult {

    private final List<Long> newIDs;

    public InsertIntoResult(long affectedLines, List<Long> newIDs) {
      super(affectedLines);
      this.newIDs = newIDs;
    }

    @Override
    public JSONObject toJSON() {
      JSONObject obj = super.toJSON();
      obj.put("newIDs", newIDs);
      return obj;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class EmptyResult extends Result {

    public EmptyResult() {
      super(true);
    }

    @Override
    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      return obj;
    }
  }

  public static final Result EMPTY = new EmptyResult();

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class MetaStatementResult extends Result {

    public MetaStatementResult() {
      super(true);
    }

    @Override
    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("type", "statement");
      obj.put("lines", "1");
      obj.put("msg", "Statement affected 1 table.");
      return obj;
    }
  }

  public static final Result META_OK = new MetaStatementResult();

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ErrorResult extends Result {

    private final Exception error;

    public ErrorResult(Exception error) {
      super(false);
      this.error = error;
    }

    @Override
    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      JSONObject e = new JSONObject();
      if (error instanceof ModelException) {
        ModelException me = (ModelException) error;
        e.put("msg", me.getSimpleMessage());
        e.put("loc", me.getLoc().toString());
      } else {
        e.put("msg", error.getMessage());
      }
      obj.put("error", e);
      return obj;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CrashResult extends Result {

    private final Throwable error;

    public CrashResult(Throwable error) {
      super(false);
      this.error = error;
    }

    @Override
    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      JSONObject e = new JSONObject();
      e.put("msg", error.getMessage());
      obj.put("error", e);
      return obj;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TransactionResult extends Result {

    private final ImmutableList<Result> results;

    public TransactionResult(Iterable<Result> results) {
      super(true);
      this.results = ImmutableList.copyOf(results);
    }

    @Override
    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      JSONArray list = new JSONArray();
      for (Result result : results) {
        list.put(result.toJSON());
      }
      obj.put("result", list);
      return obj;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class WaitResult extends Result {

    private final long startTime;
    private final long endTime;
    private final Optional<String> tag;

    public WaitResult(long startTime, long endTime, Optional<String> tag) {
      super(true);
      this.startTime = startTime;
      this.endTime = endTime;
      this.tag = tag;
    }

    @Override
    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("start_time", startTime);
      obj.put("end_time", endTime);
      if (tag.isPresent()) {
        obj.put("tag", tag.get());
      }
      obj.put("type", "statement");
      String tagStr = tag.map(t -> " (" + t + ")").orElse("");
      obj.put("msg", String.format("Statement lasted from %s to %s%s.",
          DateFunctions.sdf1.format(new Date(startTime)),
          DateFunctions.sdf1.format(new Date(endTime)),
          tagStr));
      return obj;
    }
  }
}
