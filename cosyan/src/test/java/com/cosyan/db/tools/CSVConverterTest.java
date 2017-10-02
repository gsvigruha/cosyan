package com.cosyan.db.tools;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.util.Optional;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.cosyan.db.conf.Config;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.lock.LockManager;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.sql.Lexer;
import com.cosyan.db.sql.Parser;
import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.SelectStatement.Select;
import com.cosyan.db.sql.SyntaxTree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class CSVConverterTest {

  private static MetaRepo metaRepo;
  private static Parser parser;
  private static Lexer lexer;

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParseException {
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp");
    metaRepo = new MetaRepo(new Config(props), new LockManager());
    parser = new Parser();
    lexer = new Lexer();
  }

  private ExposedTableMeta query(String sql) throws ModelException, ParserException {
    ImmutableList<Statement> tree = parser.parseStatements(lexer.tokenize(sql));
    return ((Select) Iterables.getOnlyElement(tree)).compileTable(metaRepo);
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
