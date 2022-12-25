/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.impala.parse;


import java.util.List;
import com.google.common.base.Preconditions;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.ParserRuleReturnScope;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.ql.parse.ASTErrorNode;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.GenericHiveLexer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ImmutableCommonToken;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * HiveSnippetParser allows parsing for individual tokens of statements
 * to help create ASTNodes for statements parsed by the Impala parser.
 *
 * The code is almost the same as what can be found in ParseDriver.java, but
 * having the code here prevents potential merging issues from upstream.
 */
public class HiveSnippetParser {
  private static final Logger LOG = LoggerFactory.getLogger(HiveSnippetParser.class);

  /**
   * Tree adaptor for making antlr return ASTNodes instead of CommonTree nodes
   * so that the graph walking algorithms and the rules framework defined in
   * ql.lib can be used with the AST Nodes.
   */
  public static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
    /**
     * Creates an ImpalaASTNode for the given token. The ASTNode is a wrapper around
     * antlr's CommonTree class that implements the Node interface. The ImpalaASTNode
     * derives from ASTNode and is compatible in the Hive framework. The tokens
     * are matched via the normalizeImpalaTokens script called at compile time.
     *
     * @param payload
     *          The token.
     * @return Object (which is actually an ASTNode) for the token.
     */
    @Override
    public Object create(Token payload) {
      return new ASTNode(payload);
    }

    @Override
    public Token createToken(int tokenType, String text) {
      return new ImmutableCommonToken(tokenType, text);
    }

    @Override
    public Object dupNode(Object t) {
      return create(((CommonTree)t).token);
    }

    @Override
    public Object dupTree(Object t, Object parent) {
      // Overriden to copy start index / end index, that is needed through optimization,
      // e.g., for masking/filtering
      ASTNode astNode = (ASTNode) t;
      ASTNode astNodeCopy = (ASTNode) super.dupTree(t, parent);
      astNodeCopy.setTokenStartIndex(astNode.getTokenStartIndex());
      astNodeCopy.setTokenStopIndex(astNode.getTokenStopIndex());
      return astNodeCopy;
    }

    @Override
    public Object errorNode(TokenStream input, Token start, Token stop, RecognitionException e) {
      return new ASTErrorNode(input, start, stop, e);
    }
  };

  /**
   * Parse a token snippet. For instance, if TOK_TABLE_PARTITION is passed in,
   * the expected syntax will contain the table and an optional partition definition.
   */
  public static ASTNode parse(String snippet, int tokenType)
      throws ParseException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsing snippet: " + snippet);
    }

    GenericHiveLexer lexer = GenericHiveLexer.of(snippet, null);
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    HiveParser parser = new HiveParser(tokens);
    parser.setTreeAdaptor(adaptor);
    parser.setHiveConf(null);
    ParserRuleReturnScope r;
    try {
      switch (tokenType) {
        case HiveParser.TOK_TABLE_PARTITION:
          // Call just the tableOrPartition portion of the parser here.
          r = parser.tableOrPartition();
          break;
        default:
          Preconditions.checkArgument(false, "Bad token type: " + tokenType);
          // Unreachable statement
          r = null;
      }
    } catch (RecognitionException e) {
      // Internal error if this is reached.
      throw new RuntimeException(e);
    }

    // Internal errors if exception is thrown.
    if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    }

    return (ASTNode) r.getTree();
  }
}
