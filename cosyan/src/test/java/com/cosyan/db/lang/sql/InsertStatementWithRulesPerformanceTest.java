package com.cosyan.db.lang.sql;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.QueryResult;

public class InsertStatementWithRulesPerformanceTest extends UnitTestBase {

  private static final int N1 = 5000;
  private static final int N2 = 20000;
  private static final int T = 1000;

  @Test
  public void testInsertWithRefRule() {
    execute("create table t1 (a varchar, b integer, constraint pk_a primary key (a));");
    for (int i = 0; i < N1; i++) {
      execute("insert into t1 values ('abc" + i + "' ," + i + ");");
    }
    execute("create table t2 (a varchar, c integer, "
        + "constraint fk_a foreign key (a) references t1(a),"
        + "constraint c_1 check (fk_a.b = c));");
    long t = System.currentTimeMillis();
    for (int i = 0; i < N2 / T; i++) {
      StringBuffer sb = new StringBuffer();
      for (int n = 0; n < T; n++) {
        int j = (n + i * T) % N1;
        sb.append("insert into t2 values ('abc" + j + "' ," + j + ");");
      }
      execute(sb.toString());
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with ref rules to log table (1 col) inserted in " + t + " " + speed(t, N2));
  }

  @Test
  public void testInsertWithReverseRefRule_N4() {
    execute("create table t3 (a varchar, constraint pk_a primary key (a));");
    execute("create table t4 (a varchar, b integer, "
        + "constraint fk_a foreign key (a) references t3(a));");
    execute("alter table t3 add ref s (select sum(b) as sb from rev_fk_a);");
    execute("alter table t3 add constraint c_1 check (s.sb <= 10);");

    for (int i = 0; i < N1; i++) {
      execute("insert into t3 values ('abc" + i + "');");
    }
    long t = System.currentTimeMillis();
    for (int i = 0; i < N2 / T; i++) {
      StringBuffer sb = new StringBuffer();
      for (int n = 0; n < T; n++) {
        int j = (n + i * T) % N1;
        sb.append("insert into t4 values ('abc" + j + "', 1);");
      }
      execute(sb.toString());
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with aggregating rules (4N) inserted in " + t + " " + speed(t, N2));
    QueryResult r = query("select avg(s.sb) from t3;");
    assertValues(new Object[][] { { 4.0 } }, r);
  }

  @Test
  public void testInsertWithRefRule_MultipleFields() {
    execute("create table t5 (a varchar, b integer, c integer, d integer, constraint pk_a primary key (a));");
    for (int i = 0; i < N1; i++) {
      execute("insert into t5 values ('abc" + i + "' ," + i + ", " + i + ", " + i + ");");
    }
    execute("create table t6 (a varchar, "
        + "constraint fk_a foreign key (a) references t5(a),"
        + "constraint c_1 check (fk_a.b * 2 = fk_a.c + fk_a.d));");
    long t = System.currentTimeMillis();
    for (int i = 0; i < N2 / T; i++) {
      StringBuffer sb = new StringBuffer();
      for (int n = 0; n < T; n++) {
        int j = ((n + i * T) * 193) % N1;
        sb.append("insert into t6 values ('abc" + j + "');");
      }
      execute(sb.toString());
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with ref rules to log table (3 col) inserted in " + t + " " + speed(t, N2));
  }

  @Test
  public void testInsertWithRefRule_MultipleFields_LookupTable() {
    execute("create lookup table t7 (a varchar, b integer, c integer, d integer, constraint pk_a primary key (a));");
    for (int i = 0; i < N1; i++) {
      execute("insert into t7 values ('abc" + i + "' ," + i + ", " + i + ", " + i + ");");
    }
    execute("create table t8 (a varchar, "
        + "constraint fk_a foreign key (a) references t7(a),"
        + "constraint c_1 check (fk_a.b * 2 = fk_a.c + fk_a.d));");
    long t = System.currentTimeMillis();
    for (int i = 0; i < N2 / T; i++) {
      StringBuffer sb = new StringBuffer();
      for (int n = 0; n < T; n++) {
        int j = ((n + i * T) * 193) % N1;
        sb.append("insert into t8 values ('abc" + j + "');");
      }
      execute(sb.toString());
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with ref rules to lookup table (3 col) inserted in " + t + " " + speed(t, N2));
  }

  @Test
  public void testInsertWithReverseRefRule_N400() {
    execute("create table t9 (a varchar, constraint pk_a primary key (a));");
    execute("create table t10 (a varchar, b integer, "
        + "constraint fk_a foreign key (a) references t9(a));");
    execute("alter table t9 add ref s (select sum(b) as sb from rev_fk_a);");
    execute("alter table t9 add constraint c_1 check (s.sb <= 1000);");

    int N1_2 = N1 / 100;
    for (int i = 0; i < N1_2; i++) {
      execute("insert into t9 values ('abc" + i + "');");
    }
    long t = System.currentTimeMillis();
    for (int i = 0; i < N2 / T; i++) {
      StringBuffer sb = new StringBuffer();
      for (int n = 0; n < T; n++) {
        int j = (n + i * T) % N1_2;
        sb.append("insert into t10 values ('abc" + j + "', 1);");
      }
      execute(sb.toString());
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with aggregating rules (400N) inserted in " + t + " " + speed(t, N2));
    QueryResult r = query("select avg(s.sb) from t9;");
    assertValues(new Object[][] { { 400.0 } }, r);
  }
}
