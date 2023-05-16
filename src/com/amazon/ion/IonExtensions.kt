package com.amazon.ion

import java.math.BigInteger
import java.time.Instant

/**
 * Returns `this` value, without annotations.
 * If this value has no annotations, returns `this`;
 * otherwise, returns a clone of `this` with the annotations removed.
 */
inline fun <reified T : IonValue> T.withoutTypeAnnotations(): T =
    if (typeAnnotations.isNotEmpty()) {
        clone().apply { clearTypeAnnotations() } as T
    } else {
        this
    }

/**
 * Makes an IonValue instance read-only.
 */
fun <T : IonValue> T.markReadOnly(): T {
    this.makeReadOnly()
    return this
}

/**
 * Create a [Timestamp] from [Instant] with millisecond precision and UTC offset.
 */
fun Instant.toTimestamp(): Timestamp = Timestamp.forMillis(this.toEpochMilli(), 0)

/**
 * Convert an [Timestamp] into an [Instant], truncating any fractional seconds smaller than millisecond precision.
 */
fun Timestamp.toInstant(): Instant = Instant.ofEpochMilli(this.millis)

/**
 * Kotlin-friendly extension function for building a new [IonStruct].
 */
inline fun ValueFactory.buildStruct(init: IonStruct.() -> Unit): IonStruct = newEmptyStruct().apply(init)

/**
 * Kotlin-friendly extension function for building a new [IonSexp].
 */
inline fun ValueFactory.buildSexp(init: IonSexp.() -> Unit): IonSexp = newEmptySexp().apply(init)

/**
 * Kotlin-friendly extension function for building a new [IonList].
 */
inline fun ValueFactory.buildList(init: IonList.() -> Unit): IonList = newEmptyList().apply(init)

/**
 * Returns the value of an [IonString] or [IonSymbol], or `null` if this [IonValue] is another type or an Ion null.
 */
fun IonValue.stringValueOrNull(): String? = (this as? IonText)?.stringValue()

/**
 * Returns the value of an [IonBlob] or [IonClob], or `null` if this [IonValue] is another type or an Ion null.
 */
fun IonValue.bytesValueOrNull(): ByteArray? = (this as? IonLob)?.bytes

/**
 * Returns the value of an [IonBool] or `null` if this [IonValue] is another type or an Ion null.
 */
fun IonValue.boolValueOrNull(): Boolean? = (this as? IonBool)?.booleanValue()

/**
 * Returns the value of an [IonInt] or `null` if this [IonValue] is another type or an Ion null.
 */
fun IonValue.bigIntValueOrNull(): BigInteger? = (this as? IonInt)?.bigIntegerValue()

/**
 * Returns the value of an [IonFloat] or `null` if this [IonValue] is another type or an Ion null.
 */
fun IonValue.doubleValueOrNull(): Double? = (this as? IonFloat)?.doubleValue()

/**
 * Returns the value of an [IonDecimal] or `null` if this [IonValue] is another type or an Ion null.
 */
fun IonValue.decimalValueOrNull(): Decimal? = (this as? IonDecimal)?.decimalValue()

/**
 * Returns the value of an [IonTimestamp] or `null` if this [IonValue] is another type or an Ion null.
 */
fun IonValue.timestampValueOrNull(): Timestamp? = (this as? IonTimestamp)?.timestampValue()

/**
 * Returns the child values of an [IonList] or [IonSexp] or `null` if this [IonValue] is another type or an Ion null.
 */
fun IonValue.childValuesOrNull(): List<IonValue>? = (this as? IonSequence)?.takeIf { !it.isNullValue }

/**
 * Returns the fields of an [IonStruct] or `null` if this [IonValue] is another type or an Ion null.
 */
fun IonValue.fieldsOrNull(): List<Pair<String, IonValue>>? = (this as? IonStruct)?.takeIf { !it.isNullValue }?.map { it.fieldName to it }

/**
 * If this [IonValue] is a container type, returns an iterator over the contained values. Otherwise returns an iterator
 * containing this [IonValue].
 */
fun IonValue.asIterator(): Iterator<IonValue> {
    return when (this) {
        is IonContainer -> iterator()
        else -> SingletonIterator(this)
    }
}

/**
 * An iterator over one value.
 */
private class SingletonIterator<E: Any>(value: E): Iterator<E> {
    private var nextValue: E? = value

    override fun hasNext(): Boolean = nextValue != null

    override fun next(): E = nextValue.also { nextValue = null } ?: throw NoSuchElementException()
}
