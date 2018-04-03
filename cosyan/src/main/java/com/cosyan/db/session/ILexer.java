package com.cosyan.db.session;

import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.session.IParser.ParserException;
import com.google.common.collect.PeekingIterator;

public interface ILexer {

  PeekingIterator<Token> tokenize(String sql) throws ParserException;

}
