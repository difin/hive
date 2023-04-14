package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter.emptyPlan;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class TestASTConverter {
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyPlanWhenInputSchemaIsEmpty() {
    emptyPlan(new RelRecordType(Collections.emptyList()));
  }

  @Test
  public void testEmptyPlan() {
    List<RelDataTypeField> fields = asList(
        new RelDataTypeFieldImpl("a", 0, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER)),
        new RelDataTypeFieldImpl("b", 1, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.CHAR, 30)),
        new RelDataTypeFieldImpl("c", 2, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.NULL)));
    RelDataType dataType = new RelRecordType(fields);

    ASTNode tree = emptyPlan(dataType);

    assertThat(tree.dump(), is(EXPECTED_TREE));
  }

  private static final String EXPECTED_TREE = "\n" +
      "TOK_QUERY\n" +
      "   TOK_INSERT\n" +
      "      TOK_DESTINATION\n" +
      "         TOK_DIR\n" +
      "            TOK_TMP_FILE\n" +
      "      TOK_SELECT\n" +
      "         TOK_SELEXPR\n" +
      "            TOK_FUNCTION\n" +
      "               TOK_INT\n" +
      "               TOK_NULL\n" +
      "            a\n" +
      "         TOK_SELEXPR\n" +
      "            TOK_FUNCTION\n" +
      "               TOK_CHAR\n" +
      "                  30\n" +
      "               TOK_NULL\n" +
      "            b\n" +
      "         TOK_SELEXPR\n" +
      "            TOK_NULL\n" +
      "            c\n" +
      "      TOK_LIMIT\n" +
      "         0\n" +
      "         0\n";

  @Test
  public void testEmptyPlanWithComplexTypes() {
    List<RelDataTypeField> nestedStructFields = asList(
        new RelDataTypeFieldImpl("nf1", 0, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER)),
        new RelDataTypeFieldImpl("nf2", 1, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.CHAR, 30)));

    List<RelDataTypeField> structFields = asList(
        new RelDataTypeFieldImpl("f1", 0, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER)),
        new RelDataTypeFieldImpl("farray", 1,
            new ArraySqlType(new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER), true)),
        new RelDataTypeFieldImpl("fmap", 2, new MapSqlType(
            new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER),
            new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER), true)),
        new RelDataTypeFieldImpl("fstruct", 3,
            new RelRecordType(nestedStructFields)));

    List<RelDataTypeField> fields = singletonList(new RelDataTypeFieldImpl("a", 0, new RelRecordType(structFields)));
    RelDataType dataType = new RelRecordType(fields);

    ASTNode tree = emptyPlan(dataType);
    assertThat(tree.dump(), is(EXPECTED_COMPLEX_TREE));
  }

  private static final String EXPECTED_COMPLEX_TREE = "\n" +
      "TOK_QUERY\n" +
      "   TOK_INSERT\n" +
      "      TOK_DESTINATION\n" +
      "         TOK_DIR\n" +
      "            TOK_TMP_FILE\n" +
      "      TOK_SELECT\n" +
      "         TOK_SELEXPR\n" +
      "            TOK_FUNCTION\n" +
      "               named_struct\n" +
      "               f1\n" +
      "               TOK_FUNCTION\n" +
      "                  TOK_INT\n" +
      "                  TOK_NULL\n" +
      "               farray\n" +
      "               TOK_FUNCTION\n" +
      "                  array\n" +
      "                  TOK_FUNCTION\n" +
      "                     TOK_INT\n" +
      "                     TOK_NULL\n" +
      "               fmap\n" +
      "               TOK_FUNCTION\n" +
      "                  map\n" +
      "                  TOK_FUNCTION\n" +
      "                     TOK_INT\n" +
      "                     TOK_NULL\n" +
      "                  TOK_FUNCTION\n" +
      "                     TOK_INT\n" +
      "                     TOK_NULL\n" +
      "               fstruct\n" +
      "               TOK_FUNCTION\n" +
      "                  named_struct\n" +
      "                  nf1\n" +
      "                  TOK_FUNCTION\n" +
      "                     TOK_INT\n" +
      "                     TOK_NULL\n" +
      "                  nf2\n" +
      "                  TOK_FUNCTION\n" +
      "                     TOK_CHAR\n" +
      "                        30\n" +
      "                     TOK_NULL\n" +
      "            a\n" +
      "      TOK_LIMIT\n" +
      "         0\n" +
      "         0\n";
}
