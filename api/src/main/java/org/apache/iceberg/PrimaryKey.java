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

package org.apache.iceberg;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class PrimaryKey implements Serializable {
  // IDs for partition fields start at 1000
  private static final int PRIMARY_KEY_ID_START = 1000;

  private final Schema schema;

  private final PrimaryKeyField[] fields;
  private transient volatile ListMultimap<Integer, PrimaryKeyField> fieldsBySourceId;

  private PrimaryKey(Schema schema, List<PrimaryKeyField> primaryKeyFields) {
    this.schema = schema;
    this.fields = new PrimaryKeyField[primaryKeyFields.size()];
    this.fieldsBySourceId = Multimaps.newListMultimap(Maps.newHashMap(),
        () -> Lists.newArrayListWithCapacity(fields.length));
    for (int i = 0; i < primaryKeyFields.size(); i++) {
      this.fields[i] = primaryKeyFields.get(i);
      this.fieldsBySourceId.put(fields[i].sourceId(), fields[i]);
    }
  }

  public Schema schema() {
    return schema;
  }

  public List<PrimaryKeyField> fields() {
    return ImmutableList.copyOf(fields);
  }

  public List<PrimaryKeyField> getFieldsBySourceId(int fieldId) {
    return this.fieldsBySourceId.get(fieldId);
  }

  /**
   * @return a struct type for primary keys defined by this spec.
   */
  public Types.StructType primaryKeyType() {
    List<Types.NestedField> structFields = Lists.newArrayListWithExpectedSize(fields.length);

    for (int i = 0; i < fields.length; i += 1) {
      PrimaryKeyField field = fields[i];
      Type type = schema.findType(field.sourceId());
      structFields.add(Types.NestedField.optional(field.fieldId(), field.name(), type));
    }

    return Types.StructType.of(structFields);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof PrimaryKey)) {
      return false;
    }
    PrimaryKey that = (PrimaryKey) o;
    return Arrays.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(Arrays.hashCode(fields));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("fields", fields).toString();
  }

  private static final PrimaryKey NO_PRIMARY_KEY = new PrimaryKey(new Schema(), ImmutableList.of());

  public static PrimaryKey noPrimaryKey() {
    return NO_PRIMARY_KEY;
  }

  public static Builder builderFor(Schema newSchema) {
    return new Builder(newSchema);
  }

  public static class Builder {
    private final Schema schema;
    private final List<PrimaryKeyField> fields = Lists.newArrayList();
    private final Set<String> primaryKeyNames = Sets.newHashSet();

    private final AtomicInteger lastAssignedFieldId = new AtomicInteger(PRIMARY_KEY_ID_START - 1);

    private Builder(Schema schema) {
      this.schema = schema;
    }

    private int nextFieldId() {
      return lastAssignedFieldId.incrementAndGet();
    }

    private Types.NestedField findSourceColumn(String sourceName) {
      Types.NestedField sourceColumn = schema.findField(sourceName);
      Preconditions.checkArgument(sourceColumn != null, "Cannot find source column: %s", sourceName);
      return sourceColumn;
    }

    private void checkAndAddPrimaryKeyName(String name, int fieldId) {
      Preconditions.checkArgument(name != null && !name.isEmpty(),
          "Cannot use empty or null name for primary keys: %s", name);
      Types.NestedField schemaField = schema.findField(name);
      Preconditions.checkArgument(schemaField == null || schemaField.fieldId() == fieldId,
          "Cannot create identity field sourced from different field in schema: %s", name);
      Preconditions.checkArgument(!primaryKeyNames.contains(name),
          "Cannot use field name more than once for primary keys: %s", name);
      primaryKeyNames.add(name);
    }

    Builder identity(String sourceName) {
      Types.NestedField sourceColumn = findSourceColumn(sourceName);
      checkAndAddPrimaryKeyName(sourceName, sourceColumn.fieldId());
      fields.add(new PrimaryKeyField(sourceColumn.fieldId(), nextFieldId(), sourceName));
      return this;
    }

    Builder add(int sourceId, String name) {
      return add(sourceId, nextFieldId(), name);
    }

    Builder add(int sourceId, int fieldId, String name) {
      Types.NestedField column = schema.findField(sourceId);
      Preconditions.checkArgument(column != null, "Cannot find source column: %s", sourceId);
      checkAndAddPrimaryKeyName(name, column.fieldId());
      fields.add(new PrimaryKeyField(sourceId, fieldId, name));
      lastAssignedFieldId.getAndAccumulate(fieldId, Math::max);
      return this;
    }

    public PrimaryKey build() {
      return new PrimaryKey(schema, fields);
    }
  }
}
