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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.JsonUtil;

public class PrimaryKeyParser {

  private PrimaryKeyParser() {

  }

  private static final String FIELDS = "fileds";
  private static final String SOURCE_ID = "source-id";
  private static final String FIELD_ID = "field-id";
  private static final String NAME = "name";

  public static void toJson(PrimaryKey pk, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeFieldName(FIELDS);
    toJsonFields(pk, generator);
    generator.writeEndObject();
  }

  public static String toJson(PrimaryKey pk) {
    return toJson(pk, false);
  }

  public static String toJson(PrimaryKey pk, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(pk, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static void toJsonFields(PrimaryKey pk, JsonGenerator generator) throws IOException {
    generator.writeStartArray();
    for (PrimaryKeyField field : pk.fields()) {
      generator.writeStartObject();
      generator.writeStringField(NAME, field.name());
      generator.writeNumberField(SOURCE_ID, field.sourceId());
      generator.writeNumberField(FIELD_ID, field.fieldId());
      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  public static PrimaryKey fromJson(Schema schema, JsonNode json) {
    Preconditions.checkArgument(json.isObject(), "Cannot parse primary key from non-object: %s", json);
    PrimaryKey.Builder builder = PrimaryKey.builderFor(schema);
    buildFromJsonFields(builder, json.get(FIELDS));
    return builder.build();
  }

  private static void buildFromJsonFields(PrimaryKey.Builder builder, JsonNode json) {
    Preconditions.checkArgument(json.isArray(), "Cannot parse primary key fields, not an array: %s", json);

    Iterator<JsonNode> elements = json.elements();
    while (elements.hasNext()) {
      JsonNode elem = elements.next();
      Preconditions.checkArgument(elem.isObject(), "Cannot parse primary key field, not an object: %s", elem);

      String name = JsonUtil.getString(NAME, elem);
      int sourceId = JsonUtil.getInt(SOURCE_ID, elem);
      int fieldId = JsonUtil.getInt(FIELD_ID, elem);
      builder.add(sourceId, fieldId, name);
    }
  }
}
