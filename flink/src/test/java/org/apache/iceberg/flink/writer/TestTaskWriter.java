/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.writer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.flink.WordCountData;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestTaskWriter {
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final FileFormat FORMAT = FileFormat.PARQUET;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private Table table;

  @Parameterized.Parameter
  public boolean isPartitioned;

  @Parameterized.Parameters(name = "{index}: isPartitioned={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[]{true}, new Object[]{false});
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    String tableLocation = folder.getAbsolutePath();
    table = WordCountData.createTable(tableLocation, isPartitioned);
  }

  private TaskWriter<Row> createTaskWriter(int targetFileSizeBytes) {
    OutputFileFactory outputFileFactory = new OutputFileFactory(table, FORMAT, 1);
    FileAppenderFactory<Row> fileAppenderFactory = new TestFileAppenderFactory();

    return TaskWriterFactory.createTaskWriter(SPEC,
        fileAppenderFactory, outputFileFactory, targetFileSizeBytes, FORMAT, Collections.emptyList(), null);
  }

  @Test
  public void testInsertDeleteRows() throws IOException {
    TaskWriter<Row> taskWriter = createTaskWriter(1024);

    Row row0 = Row.of("apache", 1);
    taskWriter.insert(row0);
    taskWriter.delete(row0);

    Row row1 = Row.of("software", 2);
    taskWriter.insert(row1);

    Row row2 = Row.of("foundation", 3);
    taskWriter.insert(row2);
    taskWriter.delete(row2);

    taskWriter.close();

    List<DataFile> completeFiles = taskWriter.getCompleteFiles();
    Assert.assertEquals(completeFiles.size(), 2);
    DataFile insertDataFile = completeFiles.get(0);
    DataFile deleteDataFile = completeFiles.get(1);
    Assert.assertEquals(DataFile.DataFileType.DATA_BASE_FILE, insertDataFile.dataFileType());
    Assert.assertEquals(DataFile.DataFileType.DELETE_DIFF_FILE, deleteDataFile.dataFileType());

    // Comment the transaction.
    AppendFiles append = table.newAppend();
    for (DataFile dataFile : completeFiles) {
      append.appendFile(dataFile);
    }
    append.commit();

    table.refresh();
    List<ManifestFile> manifestFiles = table.currentSnapshot().manifests();
    Assert.assertEquals(manifestFiles.size(), 2);
    Assert.assertEquals(ManifestFile.ManifestType.DATA_FILES, manifestFiles.get(0).manifestType());
    Assert.assertEquals(ManifestFile.ManifestType.DELETE_FILES, manifestFiles.get(1).manifestType());
  }

  @Test
  public void testGetCompleteFiles() throws IOException {
    TaskWriter<Row> taskWriter = createTaskWriter(1024);

    Row row0 = Row.of("apache", 1);
    taskWriter.insert(row0);
    taskWriter.delete(row0);

    AssertHelpers.assertThrows("Should throw exception if still have some unclosed files.",
        IllegalArgumentException.class,
        "Should close all opening task appenders firstly.",
        taskWriter::getCompleteFiles);

    taskWriter.close();
    List<DataFile> dataFiles = taskWriter.getCompleteFiles();
    Assert.assertEquals(2, dataFiles.size());
    Assert.assertEquals(DataFile.DataFileType.DATA_BASE_FILE, dataFiles.get(0).dataFileType());
    Assert.assertEquals(1L, dataFiles.get(0).recordCount());
    Assert.assertEquals(DataFile.DataFileType.DELETE_DIFF_FILE, dataFiles.get(1).dataFileType());
    Assert.assertEquals(1L, dataFiles.get(1).recordCount());

    taskWriter.reset();
    // This assert will make sure that the complete files are deeply cloned.
    Assert.assertEquals(2, dataFiles.size());
    Assert.assertEquals(0, taskWriter.getCompleteFiles().size());

    Row row3 = Row.of("foundation", 1);
    taskWriter.insert(row3);
    taskWriter.delete(row3);
    taskWriter.close();
    dataFiles = taskWriter.getCompleteFiles();
    Assert.assertEquals(2, dataFiles.size());
    Assert.assertEquals(DataFile.DataFileType.DATA_BASE_FILE, dataFiles.get(0).dataFileType());
    Assert.assertEquals(DataFile.DataFileType.DELETE_DIFF_FILE, dataFiles.get(1).dataFileType());
  }

  @Ignore
  @Test
  public void testRollIfReachingTargetFileSize() throws IOException {
    TaskWriter<Row> taskWriter = createTaskWriter(5);
    taskWriter.insert(Row.of("abcdef", 1));
    taskWriter.insert(Row.of("a", 2));
    taskWriter.close();
    List<DataFile> dataFiles = taskWriter.getCompleteFiles();
    Assert.assertEquals(2, dataFiles.size());
    Assert.assertEquals(DataFile.DataFileType.DATA_BASE_FILE, dataFiles.get(0).dataFileType());
    Assert.assertEquals(DataFile.DataFileType.DATA_BASE_FILE, dataFiles.get(1).dataFileType());

    taskWriter.delete(Row.of("abcdef", 1));
    taskWriter.delete(Row.of("a", 2));
    taskWriter.close();
    List<DataFile> deleteDiff = taskWriter.getCompleteFiles();
    Assert.assertEquals(2, deleteDiff.size());
    Assert.assertEquals(DataFile.DataFileType.DELETE_DIFF_FILE, deleteDiff.get(0).dataFileType());
    Assert.assertEquals(DataFile.DataFileType.DELETE_DIFF_FILE, deleteDiff.get(0).dataFileType());
  }

  private class TestFileAppenderFactory implements FileAppenderFactory<Row> {

    @Override
    public FileAppender<Row> newAppender(OutputFile outputFile, FileFormat fileFormat) {
      Map<String, String> props = table.properties();
      MetricsConfig metricsConfig = MetricsConfig.fromProperties(props);
      try {
        return Parquet.write(outputFile)
            .createWriterFunc(FlinkParquetWriters::buildWriter)
            .setAll(props)
            .metricsConfig(metricsConfig)
            .schema(table.schema())
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }
  }
}
