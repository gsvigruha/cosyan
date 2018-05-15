package com.cosyan.db.tools;

import java.io.IOException;

import org.junit.BeforeClass;

import com.cosyan.db.auth.LocalUsers;
import com.cosyan.db.conf.Config;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lang.sql.SelectStatement;
import com.cosyan.db.lock.LockManager;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.session.IParser.ParserException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class CSVConverterTest {

  private static MetaRepo metaRepo;
  private static Parser parser;
  private static Lexer lexer;

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParserException, ConfigException, DBException {
    Config config = new Config("/tmp/data");
    metaRepo = new MetaRepo(config, new LockManager(), new LocalUsers(), new Lexer(), new Parser());
    parser = new Parser();
    lexer = new Lexer();
  }

  private ExposedTableMeta query(String sql) throws ModelException, ParserException {
    ImmutableList<Statement> tree = parser.parseStatements(lexer.tokenize(sql));
    return ((SelectStatement) Iterables.getOnlyElement(tree)).getSelect().compileTable(metaRepo);
  }

  /*
  @Test
  @Ignore
  public void testReadFromCSV() throws Exception {
    CSVConverter csvConverter = new CSVConverter(metaRepo);
    csvConverter.convertWithSchema(
        "table",
        "target/test-classes/simple.csv",
        ImmutableMap.of(
            "a", new BasicColumn(0, "a", DataTypes.StringType),
            "b", new BasicColumn(1, "b", DataTypes.LongType),
            "c", new BasicColumn(2, "c", DataTypes.DoubleType)),
        Optional.empty(),
        Optional.empty());
    ExposedTableMeta ExposedTableMeta = query("select * from table;");
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "abc", "b", 1L, "c", 1.0), reader.readColumns());
  }
  */
}
