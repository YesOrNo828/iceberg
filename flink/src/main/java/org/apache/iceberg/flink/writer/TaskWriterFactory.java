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

import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.flink.PartitionKey;

import java.util.List;

public class TaskWriterFactory {

  private TaskWriterFactory() {
  }

  public static TaskWriter<Row> createTaskWriter(PartitionSpec spec,
                                                 FileAppenderFactory<Row> fileAppenderFactory,
                                                 OutputFileFactory outputFileFactory,
                                                 long targetFileSizeBytes,
                                                 FileFormat fileFormat,
                                                 List<Integer> localTimeZoneFieldIndexes,
                                                 String[] flinkFields) {
    final TaskAppender<Row> insertOnlyAppender;
    final TaskAppender<Row> deleteOnlyAppender;

    if (spec.fields().isEmpty()) {
      insertOnlyAppender = new UnpartitionedAppender<>(fileAppenderFactory,
          outputFileFactory::newOutputFile,
          targetFileSizeBytes,
          fileFormat,
          DataFile.DataFileType.DATA_BASE_FILE,
          localTimeZoneFieldIndexes);
      deleteOnlyAppender = new UnpartitionedAppender<>(fileAppenderFactory,
          outputFileFactory::newOutputFile,
          targetFileSizeBytes,
          fileFormat,
          DataFile.DataFileType.DELETE_DIFF_FILE,
          localTimeZoneFieldIndexes);
    } else {
      PartitionKey.Builder builder = new PartitionKey.Builder(spec).withFlinkSchema(flinkFields);
      insertOnlyAppender = new PartitionAppender<>(spec,
          fileAppenderFactory,
          outputFileFactory::newOutputFile,
          builder::build,
          targetFileSizeBytes,
          fileFormat,
          DataFile.DataFileType.DATA_BASE_FILE,
          localTimeZoneFieldIndexes);
      deleteOnlyAppender = new PartitionAppender<>(spec,
          fileAppenderFactory,
          outputFileFactory::newOutputFile,
          builder::build,
          targetFileSizeBytes,
          fileFormat,
          DataFile.DataFileType.DELETE_DIFF_FILE,
          localTimeZoneFieldIndexes);
    }

    return new TaskWriterImpl<>(insertOnlyAppender, deleteOnlyAppender);
  }
}
