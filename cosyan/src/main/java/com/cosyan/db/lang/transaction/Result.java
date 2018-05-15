package com.cosyan.db.lang.transaction;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import com.cosyan.db.model.DateFunctions;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public abstract class Result {

  private final boolean success;

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class QueryResult extends Result {

    private final ImmutableList<String> header;
    private final ImmutableList<Object[]> values;

    public QueryResult(Iterable<String> header, Iterable<Object[]> values) {
      super(true);
      this.header = ImmutableList.copyOf(header);
      this.values = ImmutableList.copyOf(values);
    }

    public List<List<Object>> listValues() {
      return values.stream().map(v -> Arrays.asList(v)).collect(Collectors.toList());
    }

    public static String prettyPrint(Object[] values) {
      StringJoiner vsj = new StringJoiner(",");
      for (Object obj : values) {
        if (obj == null) {
          vsj.add("null");
        } else if (obj instanceof Date) {
          vsj.add(DateFunctions.sdf1.format((Date) obj));
        } else {
          vsj.add(obj.toString());
        }
      }
      return vsj.toString() + "\n";
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
        sb.append(prettyPrint(row));
      }
      return sb.toString();
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
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class EmptyResult extends Result {

    public EmptyResult() {
      super(true);
    }
  }

  public static final Result EMPTY = new EmptyResult();

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class MetaStatementResult extends Result {

    public MetaStatementResult() {
      super(true);
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
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CrashResult extends Result {

    private final Throwable error;

    public CrashResult(Throwable error) {
      super(false);
      this.error = error;
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
  }
}
