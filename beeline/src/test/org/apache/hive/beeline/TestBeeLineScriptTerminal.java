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

package org.apache.hive.beeline;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.terminal.impl.AbstractPosixTerminal;
import org.jline.terminal.spi.TerminalExt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Regression test for HIVE-30241.
 *
 */
public class TestBeeLineScriptTerminal {

  @RepeatedTest(6)
  public void testScriptFileReaderPreservesLines() throws Exception {
    List<String> expected = scriptLines();
    byte[] script = scriptBytesWithoutTrailingNewline(expected);

    try (CapturingBeeLine beeLine = new CapturingBeeLine()) {
      ByteArrayOutputStream err = new ByteArrayOutputStream();
      beeLine.setOutputStream(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
      beeLine.setErrorStream(new PrintStream(err, true, StandardCharsets.UTF_8));
      beeLine.getOpts().setScriptFile("hive-30241.sql");

      LineReader reader = beeLine.getFileLineReader(new ByteArrayInputStream(script));
      assertScriptTerminal(reader.getTerminal());
      Assertions.assertEquals(0, beeLine.execute(reader, false));
      Assertions.assertEquals(expected.size(), beeLine.lines.size());
      for (int i = 0; i < expected.size(); i++) {
        Assertions.assertEquals(expected.get(i), beeLine.lines.get(i), "line " + i);
      }
    }
  }

  /**
   * {@code beeline -e} builds the interactive terminal before arguments are parsed. With no TTY
   * and no usable native provider (ffm is Java 22-only; jansi and jna are absent), that must
   * fall back to a dumb terminal instead of throwing {@code Unable to create a terminal}.
   */
  @Test
  public void testNoTtyFallsBackToDumbTerminal() throws Exception {
    String[] providerProps = new String[] {
        TerminalBuilder.PROP_FFM,
        TerminalBuilder.PROP_JNI,
        TerminalBuilder.PROP_JANSI,
        TerminalBuilder.PROP_JNA,
        TerminalBuilder.PROP_EXEC
    };
    Properties saved = new Properties();
    for (String prop : providerProps) {
      String value = System.getProperty(prop);
      if (value != null) {
        saved.setProperty(prop, value);
      }
      System.setProperty(prop, "false");
    }
    try (BeeLine beeLine = new BeeLine()) {
      beeLine.setOutputStream(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
      beeLine.setErrorStream(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
      Terminal terminal = beeLine.buildTerminal(null);
      try {
        Assertions.assertEquals(Terminal.TYPE_DUMB, terminal.getType());
        Assertions.assertFalse(terminal instanceof AbstractPosixTerminal);
        Assertions.assertEquals(TerminalBuilder.PROP_PROVIDER_DUMB, ((TerminalExt) terminal).getProvider().name());
      } finally {
        terminal.close();
      }
    } finally {
      for (String prop : providerProps) {
        if (saved.containsKey(prop)) {
          System.setProperty(prop, saved.getProperty(prop));
        } else {
          System.clearProperty(prop);
        }
      }
    }
  }

  /**
   * The script terminal must be the non-PTY exec terminal. These checks fail if
   * {@code buildTerminal} goes back to {@code TerminalBuilder.builder().streams(...).build()}.
   */
  private void assertScriptTerminal(Terminal terminal) {
    Assertions.assertFalse(terminal instanceof AbstractPosixTerminal,
        "script input must not use a PTY terminal (HIVE-30241), got "
            + terminal.getClass().getName());
    Assertions.assertEquals(TerminalBuilder.PROP_PROVIDER_EXEC, ((TerminalExt) terminal).getProvider().name());
    Assertions.assertEquals(Terminal.TYPE_DUMB, terminal.getType());
    Assertions.assertEquals(StandardCharsets.UTF_8, terminal.encoding());
    Assertions.assertEquals(StandardCharsets.UTF_8, terminal.inputEncoding());
    Assertions.assertEquals("beeline", terminal.getName());
  }

  private List<String> scriptLines() {
    List<String> lines = new ArrayList<>();
    lines.add("SELECT '" + "x".repeat(10_000) + "' AS long_literal;");
    for (int i = 0; i < 500; i++) {
      lines.add("EXPLAIN SELECT name, count(*) AS c_5 FROM beeline_large_script_db.t_5 "
          + "WHERE id >= 5 AND note LIKE '%--%' AND label = '中文' GROUP BY name;");
      lines.add("EXPLAIN SELECT name, count(*) AS c_5 FROM beeline_large_script_db.t_5 "
          + "WHERE id >= 5 AND note LIKE '%--%' AND label = '中文' GROUP BY name;");
      lines.add("EXPLAIN SELECT name, count(*) AS c_5 FROM beeline_large_script_db.t_5 "
          + "WHERE id >= 5 AND note LIKE '%--%' AND label = '中文' GROUP BY name;");
    }
    lines.add("SELECT 'final-without-newline' AS tail;");
    return lines;
  }

  /** Join lines with '\n' and leave the last line without a newline, as a real script file can. */
  private byte[] scriptBytesWithoutTrailingNewline(List<String> lines) {
    StringBuilder script = new StringBuilder();
    for (int i = 0; i < lines.size(); i++) {
      if (i > 0) {
        script.append('\n');
      }
      script.append(lines.get(i));
    }
    return script.toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Subclass of {@link BeeLine}. Records each line {@code execute}
   * reads instead of sending it to HiveServer2.
   */
  private static final class CapturingBeeLine extends BeeLine {
    private final List<String> lines = new ArrayList<>();

    @Override
    boolean dispatch(String line) {
      if (line == null) {
        return super.dispatch(line);
      }
      lines.add(line);
      return true;
    }
  }
}
