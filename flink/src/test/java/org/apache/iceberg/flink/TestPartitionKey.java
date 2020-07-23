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

package org.apache.iceberg.flink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;

public class TestPartitionKey {

  public static final TableSchema FLINK_SCHEMA = TableSchema.builder()
      .field("level", DataTypes.STRING())
      .field("ts", DataTypes.TIMESTAMP(3))
      .field("message", DataTypes.STRING())
      .field("sequence_number", DataTypes.BIGINT())
      .build();


  public static final TableSchema FLINK_SCHEMA_TZ = TableSchema.builder()
      .field("level", DataTypes.STRING())
      .field("ts", DataTypes.TIMESTAMP(3))
      .field("message", DataTypes.STRING())
      .field("sequence_number", DataTypes.BIGINT())
      .build();


  @Test
  public void testPartitionKey() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "level", Types.StringType.get()),
        Types.NestedField.optional(2, "sequence_number", Types.LongType.get()),
        Types.NestedField.optional(3, "message", Types.StringType.get()),
        Types.NestedField.optional(4, "ts", Types.TimestampType.withoutZone())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .hour("ts")
        .identity("level")
        .identity("sequence_number")
        .build();
    PartitionKey.Builder builder = new PartitionKey.Builder(spec).withFlinkSchema(FLINK_SCHEMA.getFieldNames());
    LocalDateTime ldt = LocalDateTime.of(2020, 6, 1, 11, 0);
    Row row = Row.of("info", ldt, "This is an info message", 100L);
    PartitionKey pk = builder.build(row);
    String actual = spec.partitionToPath(pk);
    Assert.assertEquals("should be the same", "ts_hour=2020-06-01-11/level=info/sequence_number=100", actual);
  }

  @Test
  public void testPartitionKeyTimeZone() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "level", Types.StringType.get()),
        Types.NestedField.optional(2, "sequence_number", Types.LongType.get()),
        Types.NestedField.optional(3, "message", Types.StringType.get()),
        Types.NestedField.optional(4, "ts", Types.TimestampType.withZone())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .hour("ts")
        .identity("level")
        .identity("sequence_number")
        .build();
    PartitionKey.Builder builder = new PartitionKey.Builder(spec).withFlinkSchema(FLINK_SCHEMA_TZ.getFieldNames());
    String dateFormat = "2019-11-11T10:00:01Z";
    Instant instant = Instant.parse(dateFormat);
    Row row = Row.of("info", instant, "This is an info message", 100L);
    PartitionKey pk = builder.build(row);
    String actual = spec.partitionToPath(pk);
    LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");

    String partitionHour = sdf.format(Timestamp.valueOf(ldt));
    Assert.assertEquals("should be the same", "ts_hour=" + partitionHour + "/level=info/sequence_number=100", actual);
  }

  @Test
  public void testLDT() {
    LocalDateTime localDateTime = LocalDateTime.of(2020, 6, 1, 11, 0);
    Long micros =
        localDateTime
            .toInstant(ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now()))
//                  .toInstant(ZoneOffset.UTC)
            .toEpochMilli() * 1000;
    System.out.println(micros);
    System.out.println(new Date(micros / 1000));
    Long milli =
        localDateTime
//            .toInstant(ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now()))
            .toInstant(ZoneOffset.UTC)
            .toEpochMilli();
    System.out.println(milli);
    System.out.println(new Date(milli));
  }
}
