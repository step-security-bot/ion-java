package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;

import static com.amazon.ion.util.IonStreamUtils.throwAsIonException;

public final class IonReaderBinaryIncrementalTopLevel implements IonReader, _Private_ReaderWriter {

    private final IonReaderBinaryIncrementalArbitraryDepth reader;
    private final boolean isFixed;
    private boolean isLoadingValue = false;
    private IonType type = null; // TODO see if it's possible to remove this

    IonReaderBinaryIncrementalTopLevel(IonReaderBuilder builder, InputStream inputStream) {
        reader = new IonReaderBinaryIncrementalArbitraryDepth(
            builder,
            new RefillableBufferFromInputStream(inputStream, builder.getBufferConfiguration())
        );
        isFixed = false;
    }

    IonReaderBinaryIncrementalTopLevel(IonReaderBuilder builder, byte[] data, int offset, int length) {
        reader = new IonReaderBinaryIncrementalArbitraryDepth(
            builder,
            new FixedBufferFromByteArray(data, offset, length)
        );
        isFixed = true;
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("Not implemented");
    }

    private IonCursor.Event nextHeader() {
        IonCursor.Event event = null;
        try {
            event = reader.next();
        } catch (IOException e) {
            throwAsIonException(e);
        }
        return event;
    }

    private IonCursor.Event fillValue() {
        IonCursor.Event event = null;
        try {
            event = reader.fillValue();
        } catch (IOException e) {
            throwAsIonException(e);
        }
        return event;
    }

    @Override
    public IonType next() {
        if (isFixed || !reader.isTopLevel()) {
            IonCursor.Event event = nextHeader();
            if (event == IonCursor.Event.NEEDS_DATA) {
                type = null;
                return null;
            }
        } else {
            while (true) {
                IonCursor.Event event = isLoadingValue ? fillValue() : nextHeader();
                if (event == IonCursor.Event.NEEDS_DATA) {
                    type = null;
                    return null;
                }
                isLoadingValue = true;
                event = fillValue(); // TODO this is called twice if re-entering with LOAD_VALUE as the next instruction
                if (event == IonCursor.Event.NEEDS_DATA) {
                    type = null;
                    return null;
                } else if (event == IonCursor.Event.NEEDS_INSTRUCTION) {
                    // The value was skipped for being too large. Get the next one.
                    isLoadingValue = false;
                    continue;
                }
                break;
            }
            isLoadingValue = false;
        }
        type = reader.getType();
        return type;
    }

    @Override
    public void stepIn() {
        try {
            reader.stepIn();
        } catch (IOException e) {
            throwAsIonException(e);
        }
        type = null;
    }

    @Override
    public void stepOut() {
        try {
            reader.stepOut();
        } catch (IOException e) {
            throwAsIonException(e);
        }
        type = null;
    }

    @Override
    public int getDepth() {
        return reader.getDepth();
    }

    @Override
    public SymbolTable getSymbolTable() {
        return reader.getSymbolTable();
    }

    @Override
    public IonType getType() {
        return type;
        //return reader.getType();
    }

    @Override
    public IntegerSize getIntegerSize() {
        return reader.getIntegerSize();
    }

    @Override
    public String[] getTypeAnnotations() {
        return reader.getTypeAnnotations();
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        return reader.getTypeAnnotationSymbols();
    }

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        return reader.iterateTypeAnnotations();
    }

    @Override
    public int getFieldId() {
        return reader.getFieldId();
    }

    @Override
    public String getFieldName() {
        return reader.getFieldName();
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        return reader.getFieldNameSymbol();
    }

    @Override
    public boolean isNullValue() {
        return reader.isNullValue();
    }

    @Override
    public boolean isInStruct() {
        return reader.isInStruct();
    }

    @Override
    public boolean booleanValue() {
        return reader.booleanValue();
    }

    @Override
    public int intValue() {
        return reader.intValue();
    }

    @Override
    public long longValue() {
        return reader.longValue();
    }

    @Override
    public BigInteger bigIntegerValue() {
        return reader.bigIntegerValue();
    }

    @Override
    public double doubleValue() {
        return reader.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        return reader.bigDecimalValue();
    }

    @Override
    public Decimal decimalValue() {
        return reader.decimalValue();
    }

    @Override
    public Date dateValue() {
        return reader.dateValue();
    }

    @Override
    public Timestamp timestampValue() {
        return reader.timestampValue();
    }

    @Override
    public String stringValue() {
        return reader.stringValue();
    }

    @Override
    public SymbolToken symbolValue() {
        return reader.symbolValue();
    }

    @Override
    public int byteSize() {
        return reader.byteSize();
    }

    @Override
    public byte[] newBytes() {
        return reader.newBytes();
    }

    @Override
    public int getBytes(byte[] buffer, int offset, int len) {
        return reader.getBytes(buffer, offset, len);
    }

    @Override
    public <T> T asFacet(Class<T> facetType) {
        return reader.asFacet(facetType);
    }

    public void requireCompleteValue() {
        reader.requireCompleteValue();
    }

    @Override
    public SymbolTable pop_passed_symbol_table() {
        return reader.pop_passed_symbol_table();
    }

    @Override
    public void close() throws IOException {
        requireCompleteValue();
        reader.close();
    }
}
