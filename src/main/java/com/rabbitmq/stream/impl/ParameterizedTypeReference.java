// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
//
// This software, the RabbitMQ Stream Java client library, is dual-licensed under the
// Mozilla Public License 2.0 ("MPL"), and the Apache License version 2 ("ASL").
// For the MPL, please see LICENSE-MPL-RabbitMQ. For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.
package com.rabbitmq.stream.impl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * To capture and pass a generic {@link Type}.
 *
 * <p>From the <a
 * href="https://github.com/spring-projects/spring-framework/blob/main/spring-core/src/main/java/org/springframework/core/ParameterizedTypeReference.java">Spring
 * Framework.</a>
 *
 * @param <T>
 */
public abstract class ParameterizedTypeReference<T> {

  private final Type type;

  protected ParameterizedTypeReference() {
    Class<?> parameterizedTypeReferenceSubclass =
        findParameterizedTypeReferenceSubclass(getClass());
    Type type = parameterizedTypeReferenceSubclass.getGenericSuperclass();
    ParameterizedType parameterizedType = (ParameterizedType) type;
    Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
    this.type = actualTypeArguments[0];
  }

  private static Class<?> findParameterizedTypeReferenceSubclass(Class<?> child) {
    Class<?> parent = child.getSuperclass();
    if (Object.class == parent) {
      throw new IllegalStateException("Expected ParameterizedTypeReference superclass");
    } else if (ParameterizedTypeReference.class == parent) {
      return child;
    } else {
      return findParameterizedTypeReferenceSubclass(parent);
    }
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj
        || (obj instanceof ParameterizedTypeReference
            && this.type.equals(((ParameterizedTypeReference<?>) obj).type)));
  }

  @Override
  public int hashCode() {
    return this.type.hashCode();
  }

  @Override
  public String toString() {
    return "ParameterizedTypeReference<" + this.type + ">";
  }
}
