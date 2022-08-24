package com.amazon.ion;

import com.amazon.ion.impl._Private_IncrementalReader;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public interface IonReaderReentrantCore extends IonCursor, _Private_IncrementalReader {


    /**
     * Returns the depth into the Ion value that this reader has traversed.
     * At top level the depth is 0.
     */
    int getDepth();

    /**
     * Returns the type of the current value, or null if there is no
     * current value.
     */
    IonType getType();

    /**
     * Returns an {@link IntegerSize} representing the smallest-possible
     * Java type of the Ion {@code int} at the current value.
     *
     * If the current value is {@code null.int} or is not an Ion
     * {@code int}, or if there is no current value, {@code null} will
     * be returned.
     *
     * @see IonInt#getIntegerSize()
     */
    IntegerSize getIntegerSize();

    /**
     * Determines whether the current value is a null Ion value of any type
     * (for example, <code>null</code> or <code>null.int</code>).
     * It should be called before
     * calling getters that return value types (int, long, boolean,
     * double).
     */
    boolean isNullValue();

    /**
     * Determines whether this reader is currently traversing the fields of an
     * Ion struct. It returns false if the iteration
     * is in a list, a sexp, or a datagram.
     */
    boolean isInStruct();


    /**
     * Gets the symbol ID of the field name attached to the current value.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
     *
     * @return the symbol ID of the field name, if the current value is a
     * field within a struct.
     * If the current value is not a field, or if the symbol ID cannot be
     * determined, this method returns a value <em>less than one</em>.
     *
     */
    @Deprecated
    int getFieldId();

    //=========================================================================
    // Value reading

    /**
     * Returns the current value as an boolean.
     * This is only valid when {@link #getType()} returns {@link IonType#BOOL}.
     */
    boolean booleanValue();

    /**
     * Returns the current value as an int.  This is only valid if there is
     * an underlying value and the value is of a numeric type (int, float, or
     * decimal).
     */
    int intValue();

    /**
     * Returns the current value as a long.  This is only valid if there is
     * an underlying value and the value is of a numeric type (int, float, or
     * decimal).
     */
    long longValue();

    /**
     * Returns the current value as a {@link BigInteger}.  This is only valid if there
     * is an underlying value and the value is of a numeric type (int, float, or
     * decimal).
     */
    BigInteger bigIntegerValue();

    /**
     * Returns the current value as a double.  This is only valid if there is
     * an underlying value and the value is either float, or decimal.
     */
    double doubleValue();

    /**
     * Returns the current value as a {@link BigDecimal}.
     * This method should not return a {@link Decimal}, so it lacks support for
     * negative zeros.
     * <p>
     * This method is only valid when {@link #getType()} returns
     * {@link IonType#DECIMAL}.
     *
     * @return the current value as a {@link BigDecimal},
     * or {@code null} if the current value is {@code null.decimal}.
     */
    BigDecimal bigDecimalValue();

    /**
     * Returns the current value as a {@link Decimal}, which extends
     * {@link BigDecimal} with support for negative zeros.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#DECIMAL}.
     *
     * @return the current value as a {@link Decimal},
     * or {@code null} if the current value is {@code null.decimal}.
     */
    Decimal decimalValue();


    /**
     * Returns the current value as a {@link java.util.Date}.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#TIMESTAMP}.
     *
     * @return the current value as a {@link Date},
     * or {@code null} if the current value is {@code null.timestamp}.
     */
    Date dateValue();

    /**
     * Returns the current value as a {@link Timestamp}.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#TIMESTAMP}.
     *
     * @return the current value as a {@link Timestamp},
     * or {@code null} if the current value is {@code null.timestamp}.
     */
    Timestamp timestampValue();

    /**
     * Returns the current value as a Java String.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#STRING} or {@link IonType#SYMBOL}.
     *
     * @throws UnknownSymbolException if the current value is a symbol
     * with unknown text.
     *
     * @see IonReaderReentrantApplication#symbolValue()
     */
    String stringValue();

    /**
     * Gets the size in bytes of the current lob value.
     * This is only valid when {@link #getType()} returns {@link IonType#BLOB}
     * or {@link IonType#CLOB}.
     *
     * @return the lob's size in bytes.
     */
    int byteSize();

    /**
     * Returns the current value as a newly-allocated byte array.
     * This is only valid when {@link #getType()} returns {@link IonType#BLOB}
     * or {@link IonType#CLOB}.
     */
    byte[] newBytes();

    /**
     * Copies the current value into the passed in a byte array.
     * This is only valid when {@link #getType()} returns {@link IonType#BLOB}
     * or {@link IonType#CLOB}.
     *
     * @param buffer destination to copy the value into, this must not be null.
     * @param offset the first position to copy into, this must be non null and
     *  less than the length of buffer.
     * @param len the number of bytes available in the buffer to copy into,
     *  this must be long enough to hold the whole value and not extend outside
     *  of buffer.
     */
    int getBytes(byte[] buffer, int offset, int len);

}
