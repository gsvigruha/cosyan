package com.cosyan.db.meta;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.auth.Authenticator.Method;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.session.Session;

public class GrantsTest extends UnitTestBase {

  @Test
  public void testAdminGrant() throws AuthException {
    execute("create table t1 (a varchar);");
    execute("grant select on t1 to admin;");
  }

  @Test
  public void testNewUserInsert() throws AuthException {
    execute("create table t2 (a varchar);");
    execute("create user u1 identified by 'abc';");
    Session u1 = dbApi.authSession("u1", "abc", Method.LOCAL);
    ErrorResult e = (ErrorResult) u1.execute("insert into t2 values ('x');");
    assertEquals("User 'u1' has no INSERT right on 't2'.", e.getError().getMessage());
    execute("grant insert on t2 to u1;");
    statement("insert into t2 values ('x');", u1);
  }

  @Test
  public void testNewUserSelect() throws AuthException {
    execute("create table t3 (a varchar);");
    execute("insert into t3 values ('x');");
    execute("create user u2 identified by 'abc';");
    Session u2 = dbApi.authSession("u2", "abc", Method.LOCAL);
    ErrorResult e = (ErrorResult) u2.execute("select * from t3;");
    assertEquals("User 'u2' has no SELECT right on 't3'.", e.getError().getMessage());
    execute("grant select on t3 to u2;");
    query("select * from t3;", u2);
  }

  @Test
  public void testAccessToReferencedTables() throws AuthException {
    execute("create table t4 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t5 (a varchar, constraint fk_a foreign key (a) references t4(a),"
        + "constraint c_1 check (fk_a.b > 0));");
    execute("insert into t4 values ('x', 1), ('y', 2);");
    execute("insert into t5 values ('x');");
    execute("create user u4 identified by 'abc';");
    execute("grant select on t5 to u4;");
    execute("grant insert on t5 to u4;");
    Session u4 = dbApi.authSession("u4", "abc", Method.LOCAL);
    query("select a from t5;", u4);
    ErrorResult e1 = (ErrorResult) u4.execute("insert into t5 values ('y');");
    assertEquals("User 'u4' has no SELECT right on 't4'.", e1.getError().getMessage());
    ErrorResult e2 = (ErrorResult) u4.execute("select a, fk_a.b from t5;");
    assertEquals("User 'u4' has no SELECT right on 't4'.", e2.getError().getMessage());
    execute("grant select on t4 to u4;");
    statement("insert into t5 values ('y');", u4);
    query("select a, fk_a.b from t5;", u4);
  }

  @Test
  public void testGrantAll() throws AuthException {
    execute("create table t6 (a varchar);");
    execute("insert into t6 values ('x');");
    execute("create user u5 identified by 'abc';");
    Session u5 = dbApi.authSession("u5", "abc", Method.LOCAL);
    ErrorResult e1 = (ErrorResult) u5.execute("select * from t6;");
    assertEquals("User 'u5' has no SELECT right on 't6'.", e1.getError().getMessage());
    ErrorResult e2 = (ErrorResult) u5.execute("insert into t6 values ('y');");
    assertEquals("User 'u5' has no INSERT right on 't6'.", e2.getError().getMessage());
    execute("grant all on t6 to u5;");
    query("select * from t6;", u5);
    statement("insert into t6 values ('y');");
  }

  @Test
  public void testAccessToRef() throws AuthException {
    execute("create table t7 (a varchar, constraint pk_a primary key (a));");
    execute("create table t8 (a varchar, b integer, constraint fk_a foreign key (a) references t7(a));");
    execute("alter table t7 add ref s (select sum(b) as sb from rev_fk_a);");
    execute("insert into t7 values ('x');");
    execute("insert into t8 values ('x', 1);");
    execute("create user u6 identified by 'abc';");
    execute("grant all on t7 to u6;");
    Session u6 = dbApi.authSession("u6", "abc", Method.LOCAL);
    query("select a from t7;", u6);
    ErrorResult e1 = (ErrorResult) u6.execute("select a, s.sb from t7;");
    assertEquals("User 'u6' has no SELECT right on 't8'.", e1.getError().getMessage());
    execute("grant select on t8 to u6;");
    query("select a, s.sb from t7;", u6);
  }

  @Test
  public void testAccessToEverything() throws AuthException {
    execute("create table t9 (a varchar);");
    execute("insert into t9 values ('x');");
    execute("create user u7 identified by 'abc';");
    Session u7 = dbApi.authSession("u7", "abc", Method.LOCAL);
    ErrorResult e1 = (ErrorResult) u7.execute("select * from t9;");
    assertEquals("User 'u7' has no SELECT right on 't9'.", e1.getError().getMessage());
    execute("grant all on * to u7;");
    query("select a from t9;", u7);
  }

  @Test
  public void testUserPassesOnGrant() throws AuthException {
    execute("create table t10 (a varchar);");
    execute("create user u8 identified by 'abc';");
    execute("create user u9 identified by 'abc';");
    execute("create user u10 identified by 'abc';");

    execute("grant select on t10 to u8 with grant option;");
    Session u8 = dbApi.authSession("u8", "abc", Method.LOCAL);
    metaStatement("grant select on t10 to u10;", u8);
    ErrorResult e1 = (ErrorResult) u8.execute("grant insert on t10 to u10;");
    assertEquals("User 'u8' has no grant INSERT right on 't10'.", e1.getError().getMessage());

    Session u10 = dbApi.authSession("u10", "abc", Method.LOCAL);
    ErrorResult e3 = (ErrorResult) u10.execute("grant select on t10 to u8;");
    assertEquals("User 'u10' has no grant SELECT right on 't10'.", e3.getError().getMessage());

    execute("grant all on t10 to u9 with grant option;");
    Session u9 = dbApi.authSession("u9", "abc", Method.LOCAL);
    metaStatement("grant insert on t10 to u10;", u9);

    execute("create user u11 identified by 'abc';");
    execute("grant select on * to u11 with grant option;");
    Session u11 = dbApi.authSession("u11", "abc", Method.LOCAL);
    metaStatement("grant select on t10 to u10;", u11);
    ErrorResult e2 = (ErrorResult) u11.execute("grant insert on t10 to u10;");
    assertEquals("User 'u11' has no grant INSERT right on 't10'.", e2.getError().getMessage());

    execute("create user u12 identified by 'abc';");
    execute("grant all on * to u12 with grant option;");
    Session u12 = dbApi.authSession("u12", "abc", Method.LOCAL);
    metaStatement("grant select on t10 to u10;", u12);
    metaStatement("grant insert on t10 to u10;", u12);
    metaStatement("grant all on t10 to u10;", u12);
  }

  @Test
  public void testAlterAccess() throws AuthException {
    execute("create table t11 (a varchar);");
    execute("create user u13 identified by 'abc';");
    Session u13 = dbApi.authSession("u13", "abc", Method.LOCAL);

    ErrorResult e1 = (ErrorResult) u13.execute("drop table t11;");
    assertEquals("User 'u13' has no ownership right on 't11'.", e1.getError().getMessage());
    ErrorResult e2 = (ErrorResult) u13.execute("create index t11.a;");
    assertEquals("User 'u13' has no INSERT right on 't11'.", e2.getError().getMessage());
    ErrorResult e3 = (ErrorResult) u13.execute("drop index t11.a;");
    assertEquals("User 'u13' has no ownership right on 't11'.", e3.getError().getMessage());
    ErrorResult e4 = (ErrorResult) u13.execute("alter table t11 add b varchar;");
    assertEquals("User 'u13' has no INSERT right on 't11'.", e4.getError().getMessage());

    ErrorResult e6 = (ErrorResult) u13.execute("alter table t11 add constraint c_1 check (a = 'abc');");
    assertEquals("User 'u13' has no INSERT right on 't11'.", e6.getError().getMessage());
    ErrorResult e7 = (ErrorResult) u13.execute("alter table t11 alter a varchar, immutable;");
    assertEquals("User 'u13' has no INSERT right on 't11'.", e7.getError().getMessage());
    ErrorResult e8 = (ErrorResult) u13.execute("alter table t11 drop a;");
    assertEquals("User 'u13' has no INSERT right on 't11'.", e8.getError().getMessage());

    // TODO fk
    execute("create table t12 (a varchar, constraint pk_a primary key (a));");
    execute("create table t13 (a varchar, constraint fk_a foreign key (a) references t12);");
    ErrorResult e9 = (ErrorResult) u13.execute("alter table t12 add ref s (select count(1) from rev_fk_a);");
    assertEquals("User 'u13' has no INSERT right on 't12'.", e9.getError().getMessage());

  }
}
