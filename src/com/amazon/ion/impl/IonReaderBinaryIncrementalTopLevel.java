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

public class IonReaderBinaryIncrementalTopLevel implements IonReader, _Private_ReaderWriter, _Private_IncrementalReader {

    private final IonReaderBinaryIncrementalArbitraryDepth reader;
    private IonCursor.Instruction nextInstruction = IonCursor.Instruction.NEXT_VALUE;
    private IonType type = null; // TODO see if it's possible to remove this

    IonReaderBinaryIncrementalTopLevel(IonReaderBuilder builder, InputStream inputStream) {
        reader = new IonReaderBinaryIncrementalArbitraryDepth(
            builder,
            new RefillableBufferFromInputStream(inputStream, builder.getBufferConfiguration())
        );
    }

    IonReaderBinaryIncrementalTopLevel(IonReaderBuilder builder, byte[] data, int offset, int length) {
        reader = new IonReaderBinaryIncrementalArbitraryDepth(
            builder,
            new FixedBufferFromByteArray(data, offset, length)
        );
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("Not implemented");
    }

    private IonCursor.Event next(IonCursor.Instruction instruction) {
        IonCursor.Event event = null;
        try {
            event = reader.next(instruction);
        } catch (IOException e) {
            throwAsIonException(e);
        }
        return event;
    }

    @Override
    public IonType next() {
        if (getDepth() == 0) {
            while (true) {
                IonCursor.Event event = next(nextInstruction);
                if (event == IonCursor.Event.NEEDS_DATA) {
                    type = null;
                    return null;
                }
                // TODO can the following be moved to prepareValue()?
                nextInstruction = IonCursor.Instruction.LOAD_VALUE;
                event = next(nextInstruction);
                if (event == IonCursor.Event.NEEDS_DATA) {
                    type = null;
                    return null;
                } else if (event == IonCursor.Event.NEEDS_INSTRUCTION) {
                    // The value was skipped for being too large. Get the next one.
                    nextInstruction = IonCursor.Instruction.NEXT_VALUE;
                    continue;
                }
                break;
            }
            if (nextInstruction == IonCursor.Instruction.LOAD_VALUE) {
                nextInstruction = IonCursor.Instruction.NEXT_VALUE;
            }
        } else {
            IonCursor.Event event = next(nextInstruction);
            if (event == IonCursor.Event.NEEDS_DATA) {
                type = null;
                return null;
            }
        }
        type = reader.getType();
        return type;
    }

    @Override
    public void stepIn() {
        next(IonCursor.Instruction.STEP_IN);
        type = null;
    }

    @Override
    public void stepOut() {
        next(IonCursor.Instruction.STEP_OUT);
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

    @Override
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
