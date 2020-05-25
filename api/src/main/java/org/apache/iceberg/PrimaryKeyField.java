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

import com.google.common.base.Objects;

public class PrimaryKeyField {
  private final int sourceId;
  private final int fieldId;
  private final String name;

  PrimaryKeyField(int sourceId, int fieldId, String name) {
    this.sourceId = sourceId;
    this.fieldId = fieldId;
    this.name = name;
  }

  public int sourceId() {
    return this.sourceId;
  }

  public int fieldId() {
    return this.fieldId;
  }

  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return String.format("[sourceId: %s, fieldId: %s, name: %s]", sourceId, fieldId, name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof PrimaryKeyField)) {
      return false;
    }
    PrimaryKeyField that = (PrimaryKeyField) o;
    return sourceId == that.sourceId &&
        fieldId == that.fieldId &&
        name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sourceId, fieldId, name);
  }
}
