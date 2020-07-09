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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;

class TaskWriterImpl<T> implements TaskWriter<T> {

  private final TaskAppender<T> insertOnlyAppender;
  private final TaskAppender<T> deleteOnlyAppender;
  private boolean closedWritingAppenders = true;

  TaskWriterImpl(TaskAppender<T> insertOnlyAppender, TaskAppender<T> deleteOnlyAppender) {
    this.insertOnlyAppender = insertOnlyAppender;
    this.deleteOnlyAppender = deleteOnlyAppender;
  }

  @Override
  public void insert(T record) throws IOException {
    this.closedWritingAppenders = false;
    this.insertOnlyAppender.append(record);
  }

  @Override
  public void delete(T record) throws IOException {
    this.closedWritingAppenders = false;
    this.deleteOnlyAppender.append(record);
  }

  @Override
  public void close() throws IOException {
    this.insertOnlyAppender.close();
    this.deleteOnlyAppender.close();
    this.closedWritingAppenders = true;
  }

  @Override
  public List<DataFile> getCompleteFiles() {
    Preconditions.checkArgument(closedWritingAppenders, "Should close all opening task appenders firstly.");
    List<DataFile> completeFiles = Lists.newArrayList();
    completeFiles.addAll(this.insertOnlyAppender.getCompleteFiles());
    completeFiles.addAll(this.deleteOnlyAppender.getCompleteFiles());
    return ImmutableList.copyOf(completeFiles);
  }

  @Override
  public void reset() {
    this.insertOnlyAppender.reset();
    this.deleteOnlyAppender.reset();
  }
}
