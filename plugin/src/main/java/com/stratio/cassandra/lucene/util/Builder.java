/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.util;

/**
 * Class for building complex objects.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public interface Builder<T> {

    /**
     * Returns the {@link T} represented by this builder.
     *
     * @return The {@link T} represented by this builder.
     */
    public T build();
}
