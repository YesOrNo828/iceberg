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

package org.apache.iceberg.flink.data;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.parquet.ParquetTypeVisitor;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class FlinkParquetWriters {
  private FlinkParquetWriters() {
  }

  @SuppressWarnings("unchecked")
  public static <T> ParquetValueWriter<T> buildWriter(MessageType type) {
    return (ParquetValueWriter<T>) ParquetTypeVisitor.visit(type, new WriteBuilder(type));
  }

  private static class WriteBuilder extends GenericParquetWriter.WriteBuilder {

    WriteBuilder(MessageType type) {
      super(type);
    }

    @Override
    public ParquetValueWriter<?> struct(GroupType struct,
                                        List<ParquetValueWriter<?>> fieldWriters) {
      List<Type> fields = struct.getFields();
      List<ParquetValueWriter<?>> writers = Lists.newArrayListWithExpectedSize(fieldWriters.size());
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = struct.getType(i);
        int fieldD = getMessageType().getMaxDefinitionLevel(path(fieldType.getName()));
        writers.add(ParquetValueWriters.option(fieldType, fieldD, fieldWriters.get(i)));
      }

      return new RowWriter(writers);
    }
  }

  private static class RowWriter extends ParquetValueWriters.StructWriter<Row> {

    private RowWriter(List<ParquetValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(Row row, int index) {
      return row.getField(index);
    }
  }
}
