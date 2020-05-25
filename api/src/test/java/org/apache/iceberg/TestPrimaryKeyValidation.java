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

import java.util.List;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestPrimaryKeyValidation {
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
      Types.NestedField.required(3, "website", Types.StringType.get()),
      Types.NestedField.required(4, "client_addr", Types.StringType.get())
  );

  @Test
  public void testBuildPrimaryKey() {
    PrimaryKey.Builder builder = PrimaryKey.builderFor(SCHEMA).identity("id");
    AssertHelpers.assertThrows("Duplicated primary key field.",
        IllegalArgumentException.class, "Cannot use field name more than once for primary keys: id",
        () -> builder.add(1, "id"));
    AssertHelpers.assertThrows("Not existed field name.",
        IllegalArgumentException.class, "Cannot find source column: 5",
        () -> builder.add(5, 5, "name"));
  }

  @Test
  public void testExpectedFields() {
    PrimaryKey.Builder builder = PrimaryKey.builderFor(SCHEMA).identity("id");
    PrimaryKey primaryKey = builder.add(2, "ts").build();
    List<PrimaryKeyField> primaryKeyFields = primaryKey.fields();
    Assert.assertEquals(2, primaryKeyFields.size());
    Assert.assertEquals(primaryKeyFields.get(0), new PrimaryKeyField(1, 1000, "id"));
    Assert.assertEquals(primaryKeyFields.get(1), new PrimaryKeyField(2, 1001, "ts"));

    Types.StructType expectedStruct = Types.StructType.of(
        Types.NestedField.optional(1000, "id", Types.LongType.get()),
        Types.NestedField.optional(1001, "ts", Types.TimestampType.withZone()));
    Assert.assertEquals(expectedStruct, primaryKey.primaryKeyType());
  }
}
