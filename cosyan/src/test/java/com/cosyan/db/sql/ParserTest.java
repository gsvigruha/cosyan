import org.junit.*;
import static org.junit.Assert.*;

import com.cosyan.db.sql.*;
import com.cosyan.db.sql.Parser.*;
import com.cosyan.db.sql.SyntaxTree.*;

import com.google.common.collect.ImmutableList;

import java.util.Optional;

public class ParserTest {

  private Parser parser = new Parser();

  @Test
  public void testSelect() throws ParserException {
    SyntaxTree tree = parser.parse("select * from table;");
    assertEquals(tree, new SyntaxTree(new Select(ImmutableList.of(new AllColumns()), new TableRef(new Ident("table")), Optional.empty())));
  }
}
