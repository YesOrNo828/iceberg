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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.transforms.Transform;

public class PartitionKey implements StructLike {

  private final Object[] partitionTuple;

  private PartitionKey(Object[] partitionTuple) {
    this.partitionTuple = partitionTuple;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionKey)) {
      return false;
    }

    PartitionKey that = (PartitionKey) o;
    return Arrays.equals(partitionTuple, that.partitionTuple);
  }

  public Object[] getPartitionTuple() {
    return this.partitionTuple;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(partitionTuple);
  }

  @Override
  public int size() {
    return partitionTuple.length;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(partitionTuple[pos]);
  }

  @Override
  public <T> void set(int pos, T value) {
    partitionTuple[pos] = value;
  }

  public static class Builder {
    private final int size;
    private final Transform[] transforms;
    /**
     * partition key names
     */
    private final String[] fieldNames;
    private Integer[] partitionKeyIndices;

    @SuppressWarnings("unchecked")
    public Builder(PartitionSpec spec) {
      List<PartitionField> fields = spec.fields();
      this.size = fields.size();
      this.transforms = new Transform[size];
      fieldNames = new String[size];

      for (int i = 0; i < size; i += 1) {
        PartitionField field = fields.get(i);
        this.transforms[i] = field.transform();
        String fieldName = spec.schema().findField(field.sourceId()).name();
        this.fieldNames[i] = fieldName;
      }
    }

    /**
     * flink schema
     * @param fields flink ddl field name
     * @return
     */
    public Builder withFlinkSchema(String[] fields) {
      if (fields == null) {
        if (size > 0) {
          throw new RuntimeException(String.format("iceberg partition key size:%s, but flink schema is null.", size));
        }
        return this;
      }
      partitionKeyIndices = new Integer[size];
      List<String> flinkFieldNames = Arrays.asList(fields);
      for (int i = 0; i < fieldNames.length; i++) {
        String partitionKey = fieldNames[i];
        if (flinkFieldNames.contains(partitionKey)) {
          this.partitionKeyIndices[i] = flinkFieldNames.indexOf(partitionKey);
        } else {
          throw new IllegalArgumentException(
              String.format("this partition key [%s] is not contain in flink schema %s.",
              partitionKey, flinkFieldNames));
        }
      }
      return this;
    }

    public PartitionKey build(Row row) {
      Object[] partitionTuple = new Object[size];
      for (int i = 0; i < partitionTuple.length; i += 1) {
        Transform<Object, Object> transform = transforms[i];
        Object val = row.getField(this.partitionKeyIndices[i]);
        if (val instanceof LocalDateTime) {
          LocalDateTime localDateTime = (LocalDateTime) val;
          Long micros = getMicrosFromLocalDateTime(localDateTime);
          partitionTuple[i] = transform.apply(micros);
        } else if (val instanceof Instant) {
          LocalDateTime localDateTime = LocalDateTime.ofInstant((Instant) val, ZoneId.systemDefault());
          Long micros = getMicrosFromLocalDateTime(localDateTime);
          partitionTuple[i] = transform.apply(micros);
        } else {
          partitionTuple[i] = transform.apply(val);
        }
      }
      return new PartitionKey(partitionTuple);
    }

    private Long getMicrosFromLocalDateTime(LocalDateTime localDateTime) {
      return localDateTime
              .toInstant(ZoneOffset.UTC)
              .toEpochMilli() * 1000;
    }
  }
}
