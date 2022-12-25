package org.apache.hadoop.hive.impala.parse;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.antlr.runtime.Token;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.impala.work.ImpalaWork;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.engine.EngineWork;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ImmutableCommonToken;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.impala.analysis.ResetMetadataStmt;
import org.apache.impala.analysis.PartitionKeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 Semantic analyzer for reset table metadata commands like REFRESH/INVALIDATE
 For now it works only for REFRESH command
 */
public class ResetMetadataSemanticAnalyzer extends SemanticAnalyzer {
    private static final Logger LOG = LoggerFactory.getLogger(ResetMetadataSemanticAnalyzer.class);
    public ResetMetadataSemanticAnalyzer(QueryState queryState) throws SemanticException {
        super(queryState);
    }

    @Override
    public void analyzeInternal(ASTNode root) throws SemanticException {
      try {
        LOG.debug("In Reset metadata semantic analyzer");
        assert root.getType() == ImpalaToken.TOK_REFRESH_TABLE;

        ASTNode tableNode = (ASTNode) root.getChild(0);

        // PlanUtils.addInput adds the ReadEntity object into "inputs" which is
        // used for authorization.
        Table table = getTable(tableNode);
        PlanUtils.addInput(inputs, new ReadEntity(table, null, true));

        Map<String, String> partitionSpec = getPartSpec((ASTNode) tableNode.getChild(1));
        if (partitionSpec != null) {
          PlanUtils.addInput(inputs, new ReadEntity(new Partition(table, partitionSpec, null)));
        }

        // The raw String command is passed directly to Impala for processing. The command
        // is placed into Context.optimizedSql. A little manipulation is done on the raw
        // string command to add the database qualifier if the database name is not given
        // with the table.
        String sqlStmt = ImpalaSemanticAnalyzerUtils.getQueryWithDatabase(
            (ASTNode) tableNode.getChild(0),
            queryState.getQueryString(),
            SessionState.get().getCurrentDatabase());

        queryState.setCommandType(HiveOperation.REFRESH_TABLE);
        checkEligibility(table, partitionSpec, tableNode);
        rootTasks.add(TaskFactory.get(ImpalaWork.createPlannedWork(sqlStmt, null, -1, false)));

        LOG.debug("Reset metadata analysis completed");
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
    }

    private Table getTable(ASTNode tableNode) throws SemanticException {
      String tableNameString = getUnescapedName((ASTNode) tableNode.getChild(0));
      Table table = getTable(tableNameString, true);
      TableSpec ts = new TableSpec(db, conf, tableNode, false, false);
      table.setTableSpec(ts);
      return table;
    }

    private void checkEligibility(Table tbl, Map<String, String> partitionSpec, ASTNode tableNode)
            throws SemanticException {
        assert tbl.getTTable() != null;
        if(!MetaStoreUtils.isExternalTable(tbl.getTTable())) {
            String message = "Table " + tbl.getFullyQualifiedName() + " is not an external table. " +
                    "Refresh is allowed only for external tables";
            throw new SemanticException(message);
        }
        // if partitonSpec is specified then validate that all partitions are specified
        if(partitionSpec != null) {
            validatePartSpec(tbl, partitionSpec, tableNode, conf, true);
        }
        return;
    }

    public static ASTNode getASTNode(ResetMetadataStmt stmt, String command, Context ctx)
        throws ParseException {
      Token refreshToken = new ImmutableCommonToken(
          ImpalaToken.TOK_REFRESH_TABLE, ImpalaToken.REFRESH_TABLE_STRING);
      ImpalaASTNode refreshRoot = new ImpalaASTNode(refreshToken);
      String tableAndPartitionString = StringUtils.replaceOnceIgnoreCase(command, "refresh", "");

      ASTNode tableASTNode =
          HiveSnippetParser.parse(tableAndPartitionString, HiveParser.TOK_TABLE_PARTITION);
      refreshRoot.addChild(tableASTNode);
      return refreshRoot;
    }
}
