package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReaderReentrantCore;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.IntList;
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoder;
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoderPool;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;

class IonReaderBinaryIncrementalArbitraryDepthRaw extends IonBinaryLexerBase implements IonReaderReentrantCore {

    // Isolates the highest bit in a byte.
    private static final int HIGHEST_BIT_BITMASK = 0x80;

    // Isolates the lowest seven bits in a byte.
    private static final int LOWER_SEVEN_BITS_BITMASK = 0x7F;

    private static final int SINGLE_BYTE_MASK = 0xFF;

    // Isolates the lowest six bits in a byte.
    private static final int LOWER_SIX_BITS_BITMASK = 0x3F;

    // The number of significant bits in each UInt byte.
    private static final int VALUE_BITS_PER_UINT_BYTE = 8;

    // The number of significant bits in each VarUInt byte.
    private static final int VALUE_BITS_PER_VARUINT_BYTE = 7;

    // Single byte negative zero, represented as a VarInt. Often used in timestamp encodings to indicate unknown local
    // offset.
    private static final int VAR_INT_NEGATIVE_ZERO = 0xC0;

    // The number of bytes occupied by a Java int.
    private static final int INT_SIZE_IN_BYTES = 4;

    // The number of bytes occupied by a Java long.
    private static final int LONG_SIZE_IN_BYTES = 8;

    // The smallest negative 8-byte integer that can fit in a long is -0x80_00_00_00_00_00_00_00.
    private static final int MOST_SIGNIFICANT_BYTE_OF_MIN_LONG = 0x80;

    // The largest positive 8-byte integer that can fit in a long is 0x7F_FF_FF_FF_FF_FF_FF_FF.
    private static final int MOST_SIGNIFICANT_BYTE_OF_MAX_LONG = 0x7F;

    // The second-most significant bit in the most significant byte of a VarInt is the sign.
    private static final int VAR_INT_SIGN_BITMASK = 0x40;

    // 32-bit floats must declare length 4.
    private static final int FLOAT_32_BYTE_LENGTH = 4;

    // Initial capacity of the ArrayList used to hold the symbol IDs of the annotations on the current value.
    private static final int ANNOTATIONS_LIST_INITIAL_CAPACITY = 8;

    // Converter between scalar types, allowing, for example, for a value encoded as an Ion float to be returned as a
    // Java `long` via `IonReader.longValue()`.
    private final _Private_ScalarConversions.ValueVariant scalarConverter;

    private final Utf8StringDecoder utf8Decoder = Utf8StringDecoderPool.getInstance().getOrCreate();

    // Reusable iterator over the annotation SIDs on the current value.
    private final AnnotationIterator annotationIterator;

    private long peekIndex = -1;

    // The number of bytes of a lob value that the user has consumed, allowing for piecewise reads.
    private int lobBytesRead = 0;

    // The symbol IDs for the annotations on the current value.
    private final IntList annotationSids;

    IonReaderBinaryIncrementalArbitraryDepthRaw(IonBufferConfiguration configuration, byte[] bytes, int offset, int length) {
        super(configuration, bytes, offset, length);
        scalarConverter = new _Private_ScalarConversions.ValueVariant();
        annotationSids = new IntList(ANNOTATIONS_LIST_INITIAL_CAPACITY);
        annotationIterator = new AnnotationIterator(); // TODO only if reusable is enabled?
    }

    IonReaderBinaryIncrementalArbitraryDepthRaw(IonBufferConfiguration configuration, InputStream inputStream) {
        super(configuration, inputStream);
        scalarConverter = new _Private_ScalarConversions.ValueVariant();
        annotationSids = new IntList(ANNOTATIONS_LIST_INITIAL_CAPACITY);
        annotationIterator = new AnnotationIterator(); // TODO only if reusable is enabled?
    }

    @Override
    public Event nextValue() throws IOException {
        lobBytesRead = 0;
        return super.nextValue();
    }

    ByteBuffer prepareByteBuffer(long startIndex, long endIndex) {
        // Setting the limit to the capacity first is required because setting the position will fail if the new
        // position is outside the limit.
        byteBuffer.limit(buffer.length);
        byteBuffer.position((int) startIndex);
        byteBuffer.limit((int) endIndex);
        return byteBuffer;
    }

    /**
     * Reads a VarUInt.
     * @return the value.
     */
    private int readVarUInt() { // TODO already in Lexer
        int currentByte = 0;
        int result = 0;
        while ((currentByte & HIGHEST_BIT_BITMASK) == 0) {
            currentByte = buffer[(int)(peekIndex++)];
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        }
        return result;
    }

    /**
     * Reads a UInt.
     * @param limit the position of the first byte after the end of the UInt value.
     * @return the value.
     */
    private long readUInt(long startIndex, long limit) {
        long result = 0;
        for (long i = startIndex; i < limit; i++) {
            result = (result << VALUE_BITS_PER_UINT_BYTE) | buffer[(int) i] & SINGLE_BYTE_MASK;
        }
        return result;
    }

    /**
     * Reads a VarInt.
     * @param firstByte the first byte of the VarInt representation, which has already been retrieved from the buffer.
     * @return the value.
     */
    private int readVarInt(int firstByte) {
        int currentByte = firstByte;
        int sign = (currentByte & VAR_INT_SIGN_BITMASK) == 0 ? 1 : -1;
        int result = currentByte & LOWER_SIX_BITS_BITMASK;
        while ((currentByte & HIGHEST_BIT_BITMASK) == 0) {
            currentByte = buffer[(int)(peekIndex++)];
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        }
        return result * sign;
    }

    /**
     * Reads a VarInt.
     * @return the value.
     */
    private int readVarInt() {
        return readVarInt(buffer[(int)(peekIndex++)]);
    }

    // Scratch space for various byte sizes. Only for use while computing a single value.
    private final byte[][] scratchForSize = new byte[][] {
        new byte[0],
        new byte[1],
        new byte[2],
        new byte[3],
        new byte[4],
        new byte[5],
        new byte[6],
        new byte[7],
        new byte[8],
        new byte[9],
        new byte[10],
        new byte[11],
        new byte[12],
    };

    /**
     * Copy the requested number of bytes from the buffer into a scratch buffer of exactly the requested length.
     * @param startIndex the start index from which to copy.
     * @param length the number of bytes to copy.
     * @return the scratch byte array.
     */
    private byte[] copyBytesToScratch(long startIndex, int length) {
        // Note: using reusable scratch buffers makes reading ints and decimals 1-5% faster and causes much less
        // GC churn.
        byte[] bytes = null;
        if (length < scratchForSize.length) {
            bytes = scratchForSize[length];
        }
        if (bytes == null) {
            bytes = new byte[length];
        }
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        System.arraycopy(buffer, (int) startIndex, bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * Reads a UInt value into a BigInteger.
     * @param isNegative true if the resulting BigInteger value should be negative; false if it should be positive.
     * @return the value.
     */
    private BigInteger readUIntAsBigInteger(boolean isNegative) {
        int length = (int) (valueMarker.endIndex - valueMarker.startIndex);
        // NOTE: unfortunately, there is no BigInteger(int signum, byte[] bits, int offset, int length) constructor
        // until JDK 9, so copying to scratch space is always required. Migrating to the new constructor will
        // lead to a significant performance improvement.
        byte[] magnitude = copyBytesToScratch(valueMarker.startIndex, length);
        int signum = isNegative ? -1 : 1;
        return new BigInteger(signum, magnitude);
    }

    /**
     * Get and clear the most significant bit in the given byte array.
     * @param intBytes bytes representing a signed int.
     * @return -1 if the most significant bit was set; otherwise, 1.
     */
    private int getAndClearSignBit(byte[] intBytes) {
        boolean isNegative = (intBytes[0] & HIGHEST_BIT_BITMASK) != 0;
        int signum = isNegative ? -1 : 1;
        if (isNegative) {
            intBytes[0] &= LOWER_SEVEN_BITS_BITMASK;
        }
        return signum;
    }

    /**
     * Reads an Int value into a BigInteger.
     * @param limit the position of the first byte after the end of the UInt value.
     * @return the value.
     */
    private BigInteger readIntAsBigInteger(long limit) {
        BigInteger value;
        int length = (int) (limit - peekIndex);
        if (length > 0) {
            // NOTE: unfortunately, there is no BigInteger(int signum, byte[] bits, int offset, int length) constructor
            // until JDK 9, so copying to scratch space is always required. Migrating to the new constructor will
            // lead to a significant performance improvement.
            byte[] bytes = copyBytesToScratch(peekIndex, length);
            value = new BigInteger(getAndClearSignBit(bytes), bytes);
        }
        else {
            value = BigInteger.ZERO;
        }
        return value;
    }

    @Override
    public boolean isNullValue() {
        return valueTid != null && valueTid.isNull;
    }

    @Override
    public IntegerSize getIntegerSize() {
        if (valueTid.type != IonType.INT || valueTid.isNull) {
            return null;
        }
        if (valueTid.length < INT_SIZE_IN_BYTES) {
            // Note: this is conservative. Most integers of size 4 also fit in an int, but since exactly the
            // same parsing code is used for ints and longs, there is no point wasting the time to determine the
            // smallest possible type.
            return IntegerSize.INT;
        } else if (valueTid.length < LONG_SIZE_IN_BYTES) {
            return IntegerSize.LONG;
        } else if (valueTid.length == LONG_SIZE_IN_BYTES) {
            // Because creating BigIntegers is so expensive, it is worth it to look ahead and determine exactly
            // which 8-byte integers can fit in a long.
            if (valueTid.isNegativeInt) {
                // The smallest negative 8-byte integer that can fit in a long is -0x80_00_00_00_00_00_00_00.
                int firstByte = buffer[(int)(valueMarker.startIndex)] & SINGLE_BYTE_MASK;
                if (firstByte < MOST_SIGNIFICANT_BYTE_OF_MIN_LONG) {
                    return IntegerSize.LONG;
                } else if (firstByte > MOST_SIGNIFICANT_BYTE_OF_MIN_LONG) {
                    return IntegerSize.BIG_INTEGER;
                }
                for (long i = valueMarker.startIndex + 1; i < valueMarker.endIndex; i++) {
                    if (0x00 != buffer[(int)(i)]) {
                        return IntegerSize.BIG_INTEGER;
                    }
                }
            } else {
                // The largest positive 8-byte integer that can fit in a long is 0x7F_FF_FF_FF_FF_FF_FF_FF.
                if ((buffer[(int)(valueMarker.startIndex)] & SINGLE_BYTE_MASK) > MOST_SIGNIFICANT_BYTE_OF_MAX_LONG) {
                    return IntegerSize.BIG_INTEGER;
                }
            }
            return IntegerSize.LONG;
        }
        return IntegerSize.BIG_INTEGER;
    }

    private void throwDueToInvalidType(IonType type) {
        throw new IllegalStateException(
            String.format("Invalid type. Required %s but found %s.", type, valueTid == null ? null : valueTid.type)
        );
    }

    @Override
    public int byteSize() {
        if (valueTid == null || (!IonType.isLob(valueTid.type) && !valueTid.isNull)) {
            throw new IonException("Reader must be positioned on a blob or clob.");
        }
        return (int) (valueMarker.endIndex - valueMarker.startIndex);
    }

    @Override
    public byte[] newBytes() {
        byte[] bytes = new byte[byteSize()];
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        System.arraycopy(buffer, (int) valueMarker.startIndex, bytes, 0, bytes.length);
        return bytes;
    }

    @Override
    public int getBytes(byte[] bytes, int offset, int len) {
        int length = Math.min(len, byteSize() - lobBytesRead);
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        System.arraycopy(buffer, (int) (valueMarker.startIndex + lobBytesRead), bytes, offset, length);
        lobBytesRead += length;
        return length;
    }

    /**
     * Reads a decimal value as a BigDecimal.
     * @return the value.
     */
    private BigDecimal readBigDecimal() {
        long length = (int) (valueMarker.endIndex - peekIndex);
        if (length == 0) {
            return BigDecimal.ZERO;
        }
        int scale = -readVarInt();
        BigDecimal value;
        if (length < LONG_SIZE_IN_BYTES) {
            // No need to allocate a BigInteger to hold the coefficient.
            long coefficient = 0;
            int sign = 1;
            if (peekIndex < valueMarker.endIndex) {
                int firstByte = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
                sign = (firstByte & HIGHEST_BIT_BITMASK) == 0 ? 1 : -1;
                coefficient = firstByte & LOWER_SEVEN_BITS_BITMASK;
            }
            while (peekIndex < valueMarker.endIndex) {
                coefficient = (coefficient << VALUE_BITS_PER_UINT_BYTE) | buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
            }
            value = BigDecimal.valueOf(coefficient * sign, scale);
        } else {
            // The coefficient may overflow a long, so a BigInteger is required.
            value = new BigDecimal(readIntAsBigInteger(valueMarker.endIndex), scale);
        }
        return value;
    }

    /**
     * Reads a decimal value as a Decimal.
     * @return the value.
     */
    private Decimal readDecimal() {
        int length = (int) (valueMarker.endIndex - peekIndex);
        if (length == 0) {
            return Decimal.ZERO;
        }
        int scale = -readVarInt();
        BigInteger coefficient;
        length = (int) (valueMarker.endIndex - peekIndex);
        if (length > 0) {
            // NOTE: unfortunately, there is no BigInteger(int signum, byte[] bits, int offset, int length) constructor,
            // so copying to scratch space is always required.
            byte[] bits = copyBytesToScratch(peekIndex, length);
            int signum = getAndClearSignBit(bits);
            // NOTE: there is a BigInteger.valueOf(long unscaledValue, int scale) factory method that avoids allocating
            // a BigInteger for coefficients that fit in a long. See its use in readBigDecimal() above. Unfortunately,
            // it is not possible to use this for Decimal because the necessary BigDecimal constructor is
            // package-private. If a compatible BigDecimal constructor is added in a future JDK revision, a
            // corresponding factory method should be added to Decimal to enable this optimization.
            coefficient = new BigInteger(signum, bits);
            if (coefficient.signum() == 0 && signum < 0) {
                return Decimal.negativeZero(scale);
            }
        }
        else {
            coefficient = BigInteger.ZERO;
        }
        return Decimal.valueOf(coefficient, scale);
    }

    @Override
    public BigDecimal bigDecimalValue() {
        if (valueTid == null || IonType.DECIMAL != valueTid.type) {
            throwDueToInvalidType(IonType.DECIMAL);
        }
        if (valueTid.isNull) {
            return null;
        }
        peekIndex = valueMarker.startIndex;
        return readBigDecimal();
    }

    @Override
    public Decimal decimalValue() {
        if (valueTid == null || IonType.DECIMAL != valueTid.type) {
            throwDueToInvalidType(IonType.DECIMAL);
        }
        if (valueTid.isNull) {
            return null;
        }
        peekIndex = valueMarker.startIndex;
        return readDecimal();
    }

    @Override
    public long longValue() {
        long value;
        if (valueTid.type == IonType.INT) {
            if (valueTid.length == 0) {
                return 0;
            }
            value = readUInt(valueMarker.startIndex, valueMarker.endIndex);
            if (valueTid.isNegativeInt) {
                if (value == 0) {
                    throw new IonException("Int zero may not be negative.");
                }
                value *= -1;
            }
        } else if (valueTid.type == IonType.FLOAT) {
            scalarConverter.addValue(doubleValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.double_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.long_value));
            value = scalarConverter.getLong();
            scalarConverter.clear();
        } else if (valueTid.type == IonType.DECIMAL) {
            scalarConverter.addValue(decimalValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.decimal_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.long_value));
            value = scalarConverter.getLong();
            scalarConverter.clear();
        } else {
            throw new IllegalStateException("longValue() may only be called on values of type int, float, or decimal.");
        }
        return value;
    }

    @Override
    public BigInteger bigIntegerValue() {
        BigInteger value;
        if (valueTid.type == IonType.INT) {
            if (valueTid.isNull) {
                // NOTE: this mimics existing behavior, but should probably be undefined (as, e.g., longValue() is in this
                //  case).
                return null;
            }
            if (valueTid.length == 0) {
                return BigInteger.ZERO;
            }
            value = readUIntAsBigInteger(valueTid.isNegativeInt);
            if (valueTid.isNegativeInt && value.signum() == 0) {
                throw new IonException("Int zero may not be negative.");
            }
        } else if (valueTid.type == IonType.FLOAT) {
            if (valueTid.isNull) {
                value = null;
            } else {
                scalarConverter.addValue(doubleValue());
                scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.double_value);
                scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.bigInteger_value));
                value = scalarConverter.getBigInteger();
                scalarConverter.clear();
            }
        } else if (valueTid.type == IonType.DECIMAL) {
            if (valueTid.isNull) {
                value = null;
            } else {
                scalarConverter.addValue(decimalValue());
                scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.decimal_value);
                scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.bigInteger_value));
                value = scalarConverter.getBigInteger();
                scalarConverter.clear();
            }
        } else {
            throw new IllegalStateException("longValue() may only be called on values of type int, float, or decimal.");
        }
        return value;
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public double doubleValue() {
        double value;
        if (valueTid.type == IonType.FLOAT) {
            int length = (int) (valueMarker.endIndex - valueMarker.startIndex);
            if (length == 0) {
                return 0.0d;
            }
            ByteBuffer bytes = prepareByteBuffer(valueMarker.startIndex, valueMarker.endIndex);
            if (length == FLOAT_32_BYTE_LENGTH) {
                value = bytes.getFloat();
            } else {
                // Note: there is no need to check for other lengths here; the type ID byte is validated during next().
                value = bytes.getDouble();
            }
        }  else if (valueTid.type == IonType.DECIMAL) {
            scalarConverter.addValue(decimalValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.decimal_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.double_value));
            value = scalarConverter.getDouble();
            scalarConverter.clear();
        } else {
            throw new IllegalStateException("doubleValue() may only be called on values of type float or decimal.");
        }
        return value;
    }

    @Override
    public Timestamp timestampValue() {
        if (valueTid == null || IonType.TIMESTAMP != valueTid.type) {
            throwDueToInvalidType(IonType.TIMESTAMP);
        }
        if (valueTid.isNull) {
            return null;
        }
        peekIndex = valueMarker.startIndex;
        int firstByte = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
        Integer offset = null;
        if (firstByte != VAR_INT_NEGATIVE_ZERO) {
            offset = readVarInt(firstByte);
        }
        int year = readVarUInt();
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        BigDecimal fractionalSecond = null;
        Timestamp.Precision precision = Timestamp.Precision.YEAR;
        if (peekIndex < valueMarker.endIndex) {
            month = readVarUInt();
            precision = Timestamp.Precision.MONTH;
            if (peekIndex < valueMarker.endIndex) {
                day = readVarUInt();
                precision = Timestamp.Precision.DAY;
                if (peekIndex < valueMarker.endIndex) {
                    hour = readVarUInt();
                    if (peekIndex >= valueMarker.endIndex) {
                        throw new IonException("Timestamps may not specify hour without specifying minute.");
                    }
                    minute = readVarUInt();
                    precision = Timestamp.Precision.MINUTE;
                    if (peekIndex < valueMarker.endIndex) {
                        second = readVarUInt();
                        precision = Timestamp.Precision.SECOND;
                        if (peekIndex < valueMarker.endIndex) {
                            fractionalSecond = readBigDecimal();
                            if (fractionalSecond.signum() < 0 || fractionalSecond.compareTo(BigDecimal.ONE) >= 0) {
                                throw new IonException("The fractional seconds value in a timestamp must be greater" +
                                    "than or equal to zero and less than one.");
                            }
                        }
                    }
                }
            }
        }
        try {
            return Timestamp.createFromUtcFields(
                precision,
                year,
                month,
                day,
                hour,
                minute,
                second,
                fractionalSecond,
                offset
            );
        } catch (IllegalArgumentException e) {
            throw new IonException("Illegal timestamp encoding. ", e);
        }
    }

    @Override
    public Date dateValue() {
        Timestamp timestamp = timestampValue();
        if (timestamp == null) {
            return null;
        }
        return timestamp.dateValue();
    }

    @Override
    public boolean booleanValue() {
        if (valueTid == null || IonType.BOOL != valueTid.type) {
            throwDueToInvalidType(IonType.BOOL);
        }
        return valueTid.lowerNibble == 1;
    }

    @Override
    public String stringValue() {
        if (valueTid == null || IonType.STRING != valueTid.type) {
            throwDueToInvalidType(IonType.STRING);
        }
        if (valueTid.isNull) {
            return null;
        }
        ByteBuffer utf8InputBuffer = prepareByteBuffer(valueMarker.startIndex, valueMarker.endIndex);
        return utf8Decoder.decode(utf8InputBuffer, (int) (valueMarker.endIndex - valueMarker.startIndex));
    }

    /**
     *
     * @return -1 if the value is null
     */
    public int symbolValueId() {
        if (valueTid == null || IonType.SYMBOL != valueTid.type) {
            throwDueToInvalidType(IonType.SYMBOL);
        }
        if (valueTid.isNull) {
            return -1;
        }
        return (int) readUInt(valueMarker.startIndex, valueMarker.endIndex);
    }

    /**
     * Gets the annotation symbol IDs for the current value, reading them from the buffer first if necessary.
     * @return the annotation symbol IDs, or an empty list if the current value is not annotated.
     */
    IntList getAnnotationSids() {
        annotationSids.clear();
        long savedPeekIndex = peekIndex;
        peekIndex = annotationSidsMarker.startIndex;
        while (peekIndex < annotationSidsMarker.endIndex) {
            annotationSids.add(readVarUInt());
        }
        peekIndex = savedPeekIndex;
        return annotationSids;
    }

    /**
     * Reusable iterator over the annotations on the current value.
     */
    class AnnotationIterator {

        // The byte position of the annotation to return from the next call to next().
        private long nextAnnotationPeekIndex;

        public boolean hasNext() {
            return nextAnnotationPeekIndex < annotationSidsMarker.endIndex;
        }

        public int next() {
            long savedPeekIndex = peekIndex;
            peekIndex = nextAnnotationPeekIndex;
            int sid = readVarUInt();
            nextAnnotationPeekIndex = peekIndex;
            peekIndex = savedPeekIndex;
            return sid;
        }

        /**
         * Prepare the iterator to iterate over the annotations on the current value.
         */
        void ready() {
            nextAnnotationPeekIndex = annotationSidsMarker.startIndex;
        }

        /**
         * Invalidate the iterator so that all future calls to {@link #hasNext()} will return false until the
         * next call to {@link #ready()}.
         */
        void invalidate() {
            nextAnnotationPeekIndex = Integer.MAX_VALUE;
        }
    }

    AnnotationIterator iterateAnnotationSids() {
        annotationIterator.ready();
        return annotationIterator;
    }

    @Override
    public int getFieldId() {
        return fieldSid;
    }

    @Override
    public boolean isInStruct() {
        return !containerStack.isEmpty() && containerStack.peek().type == IonType.STRUCT;
    }

    @Override
    public IonType getType() {
        return valueTid == null ? null : valueTid.type;
    }

    @Override
    public int getDepth() {
        return containerStack.size();
    }

    boolean hasAnnotations() {
        return annotationSidsMarker.startIndex >= 0;
    }

    @Override
    public void requireCompleteValue() {
        if (event == Event.NEEDS_DATA && isAwaitingMoreData()) {
            throw new IonException("Unexpected EOF.");
        }
    }

    @Override
    public void close() throws IOException {
        utf8Decoder.close();
        super.close();
    }
}
