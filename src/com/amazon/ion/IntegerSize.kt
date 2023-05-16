/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.ion

/**
 * Indicates the smallest-possible Java type for an Ion `int` value.
 */
enum class IntegerSize {
    /**
     * Fits in the Java `int` primitive (four bytes).
     * The value can be retrieved through methods like [IonReader.intValue]
     * or [IonInt.intValue] without data loss.
     */
    INT,

    /**
     * Fits in the Java `int` primitive (eight bytes).
     * The value can be retrieved through methods like [IonReader.longValue]
     * or [IonInt.longValue] without data loss.
     */
    LONG,

    /**
     * Larger than eight bytes. This value can be retrieved through methods like
     * [IonReader.bigIntegerValue] or [IonInt.bigIntegerValue]
     * without data loss.
     */
    BIG_INTEGER
}
