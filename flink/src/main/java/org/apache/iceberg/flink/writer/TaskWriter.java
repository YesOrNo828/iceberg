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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;

/**
 * The writer interface which could accept both INSERT and DELETE record.
 *
 * @param <T> to indicate the record data type.
 */
public interface TaskWriter<T> {

  /**
   * Insert the row by appending it into the insert data files.
   */
  void insert(T row) throws IOException;

  /**
   * Delete this row by appending it into the delete differential files.
   */
  void delete(T row) throws IOException;

  /**
   * Close the writer.
   */
  void close() throws IOException;

  /**
   * To get the full list of complete files, we should call this method after {@link TaskWriter#close()} because the
   * close method will close all the opening data files and build {@link DataFile} to the return array list.
   */
  List<DataFile> getCompleteFiles();

  /**
   * Reset to clear all the cached complete files.
   */
  void reset();
}
