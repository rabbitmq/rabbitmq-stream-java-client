package com.rabbitmq.stream.impl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * To capture and pass a generic {@link Type}.
 * <p>
 * From the
 * <a href="https://github.com/spring-projects/spring-framework/blob/master/spring-core/src/main/java/org/springframework/core/ParameterizedTypeReference.java">Spring Framework.</a>
 *
 * @param <T>
 */
public abstract class ParameterizedTypeReference<T> {

    private final Type type;


    protected ParameterizedTypeReference() {
        Class<?> parameterizedTypeReferenceSubclass = findParameterizedTypeReferenceSubclass(getClass());
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
        return (this == obj || (obj instanceof ParameterizedTypeReference &&
                this.type.equals(((ParameterizedTypeReference<?>) obj).type)));
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
