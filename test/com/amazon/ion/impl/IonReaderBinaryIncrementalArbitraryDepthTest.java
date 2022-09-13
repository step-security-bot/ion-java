package com.amazon.ion.impl;

import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.TestUtils;
import com.amazon.ion.impl.bin._Private_IonManagedWriter;
import com.amazon.ion.impl.bin._Private_IonRawWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.amazon.ion.IonCursor.Event.END_CONTAINER;
import static com.amazon.ion.IonCursor.Event.NEEDS_DATA;
import static com.amazon.ion.IonCursor.Event.NEEDS_INSTRUCTION;
import static com.amazon.ion.IonCursor.Event.START_CONTAINER;
import static com.amazon.ion.IonCursor.Event.START_SCALAR;
import static com.amazon.ion.IonCursor.Event.VALUE_READY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IonReaderBinaryIncrementalArbitraryDepthTest {

    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    private static final IonReaderBuilder STANDARD_READER_BUILDER = IonReaderBuilder.standard()
        .withIncrementalReadingEnabled(true);
    private static final IonBinaryWriterBuilder STANDARD_WRITER_BUILDER = IonBinaryWriterBuilder.standard();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    // Builds the incremental reader. May be overwritten by individual tests.
    private IonReaderBuilder readerBuilder;
    // Builds binary writers for constructing test data. May be overwritten by individual tests.
    private IonBinaryWriterBuilder writerBuilder;

    @Before
    public void setup() {
        readerBuilder = STANDARD_READER_BUILDER;
        writerBuilder = STANDARD_WRITER_BUILDER;
    }

    /**
     * Writes binary Ion streams with a user-level writer.
     */
    private interface WriterFunction {
        void write(IonWriter writer) throws IOException;
    }

    /**
     * Writes binary Ion streams with a raw writer. Also allows bytes to be written directly to the stream.
     */
    private interface RawWriterFunction {
        void write(_Private_IonRawWriter writer, ByteArrayOutputStream out) throws IOException;
    }

    /**
     * Converts the given text Ion to the equivalent binary Ion.
     * @param ion text Ion data.
     * @return the equivalent binary Ion data.
     * @throws Exception if the given data is invalid.
     */
    private static byte[] toBinary(String ion) throws Exception {
        return TestUtils.ensureBinary(SYSTEM, ion.getBytes("UTF-8"));
    }

    /**
     * Creates an incremental reader over the binary equivalent of the given text Ion.
     * @param ion text Ion data.
     * @return a new reader.
     * @throws Exception if an exception is raised while converting the Ion data.
     */
    private IonReaderBinaryIncrementalArbitraryDepth readerFor(String ion) throws Exception {
        return new IonReaderBinaryIncrementalArbitraryDepth(
            readerBuilder,
            new IonBinaryLexerRefillableFromInputStream(
                new ByteArrayInputStream(toBinary(ion)),
                readerBuilder.getBufferConfiguration()
            )
        );
    }

    /**
     * Creates an incremental reader over the binary Ion data created by invoking the given RawWriterFunction.
     * @param writerFunction the function used to generate the data.
     * @return a new reader.
     * @throws Exception if an exception is raised while writing the Ion data.
     */
    private IonReaderBinaryIncrementalArbitraryDepth readerFor(RawWriterFunction writerFunction) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        _Private_IonRawWriter writer = writerBuilder.build(out)
            .asFacet(_Private_IonManagedWriter.class)
            .getRawWriter();
        out.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
        writerFunction.write(writer, out);
        writer.close();
        return new IonReaderBinaryIncrementalArbitraryDepth(
            readerBuilder,
            new IonBinaryLexerRefillableFromInputStream(
                new ByteArrayInputStream(out.toByteArray()),
                readerBuilder.getBufferConfiguration()
            )
        );
    }

    /**
     * Creates an incremental reader over the binary Ion data created by invoking the given WriterFunction.
     * @param writerFunction the function used to generate the data.
     * @return a new reader.
     * @throws Exception if an exception is raised while writing the Ion data.
     */
    private IonReaderBinaryIncrementalArbitraryDepth readerFor(WriterFunction writerFunction) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = writerBuilder.build(out);
        writerFunction.write(writer);
        writer.close();
        return new IonReaderBinaryIncrementalArbitraryDepth(
            readerBuilder,
            new IonBinaryLexerRefillableFromInputStream(
                new ByteArrayInputStream(out.toByteArray()),
                readerBuilder.getBufferConfiguration()
            )
        );
    }

    /**
     * Creates an incremental reader over the given bytes, prepended with the IVM.
     * @param ion binary Ion bytes without an IVM.
     * @return a new reader.
     */
    private IonReaderBinaryIncrementalArbitraryDepth readerFor(int... ion) throws Exception {
        return new IonReaderBinaryIncrementalArbitraryDepth(
            readerBuilder,
            new IonBinaryLexerRefillableFromInputStream(
                new ByteArrayInputStream(new TestUtils.BinaryIonAppender().append(ion).toByteArray()),
                readerBuilder.getBufferConfiguration()
            )
        );
    }

    private static void drainAnnotations(IonReaderBinaryIncrementalArbitraryDepth reader, List<String> annotationsSink) {
        Iterator<String> iterator = reader.iterateTypeAnnotations();
        while (iterator.hasNext()) {
            annotationsSink.add(iterator.next());
        }
    }

    private static void assertInt(int expected, IonReaderBinaryIncrementalArbitraryDepth reader) throws IOException {
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(IonType.INT, reader.getType());
        assertTrue(reader.getIntegerSize().ordinal() >= IntegerSize.INT.ordinal());
        assertEquals(expected, reader.intValue());
    }

    private static void assertStreamEnd(IonReaderBinaryIncrementalArbitraryDepth reader) throws IOException {
        assertEquals(NEEDS_DATA, reader.nextValue());
        assertNull(null, reader.getType());
    }

    @Test
    public void annotatedTopLevelIterator() throws Exception {
        IonReaderBinaryIncrementalArbitraryDepth reader = readerFor("foo::bar::123 baz::456");
        assertEquals(START_SCALAR, reader.nextValue());
        List<String> annotations = new ArrayList<String>();
        drainAnnotations(reader, annotations);
        assertEquals(Arrays.asList("foo", "bar"), annotations);
        annotations.clear();
        assertInt(123, reader);
        assertEquals(START_SCALAR, reader.nextValue());
        drainAnnotations(reader, annotations);
        assertEquals(Collections.singletonList("baz"), annotations);
        assertInt(456, reader);
        assertStreamEnd(reader);
        reader.close();
    }

    @Test
    public void annotatedInContainer() throws Exception {
        IonReaderBinaryIncrementalArbitraryDepth reader = readerFor("[foo::bar::123, baz::456]");
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(Arrays.asList("foo", "bar"), Arrays.asList(reader.getTypeAnnotations()));
        assertInt(123, reader);
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(Collections.singletonList("baz"), Arrays.asList(reader.getTypeAnnotations()));
        assertInt(456, reader);
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertStreamEnd(reader);
        reader.close();
    }

    @Test
    public void nestedContainers() throws Exception {
        IonReaderBinaryIncrementalArbitraryDepth reader = readerFor("{abc: foo::bar::(123), def: baz::[456, {}]}");
        assertNull(reader.getType());
        assertEquals(0, reader.getDepth());
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(IonType.STRUCT, reader.getType());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertTrue(reader.isInStruct());
        assertNull(reader.getType());
        assertEquals(1, reader.getDepth());
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(IonType.SEXP, reader.getType());
        assertEquals(Arrays.asList("foo", "bar"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertFalse(reader.isInStruct());
        assertEquals(2, reader.getDepth());
        assertNull(reader.getType());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(IonType.INT, reader.getType());
        assertInt(123, reader);
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(2, reader.getDepth());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(1, reader.getDepth());
        assertNull(reader.getType());
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(IonType.LIST, reader.getType());
        assertEquals(Collections.singletonList("baz"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(2, reader.getDepth());
        assertNull(reader.getType());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(IonType.INT, reader.getType());
        assertInt(456, reader);
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(IonType.STRUCT, reader.getType());
        assertEquals(2, reader.getDepth());
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(2, reader.getDepth());
        assertNull(reader.getType());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(1, reader.getDepth());
        assertNull(reader.getType());
        assertEquals(END_CONTAINER, reader.nextValue());
        assertNull(reader.getType());
        assertEquals(1, reader.getDepth());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(0, reader.getDepth());
        assertNull(reader.getType());
        assertStreamEnd(reader);
        reader.close();
    }

    @Test
    public void skipContainers() throws Exception {
        IonReaderBinaryIncrementalArbitraryDepth reader = readerFor(
            "[123] 456 {abc: foo::bar::123, def: baz::456} [123] 789 [foo::bar::123, baz::456] [123]"
        );
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(IonType.LIST, reader.getType());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(IonType.INT, reader.getType());
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(IonType.STRUCT, reader.getType());
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(IonType.LIST, reader.getType());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertNull(reader.getType());
        assertEquals(START_SCALAR, reader.nextValue());
        assertInt(123, reader);
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertNull(reader.getType());
        assertEquals(START_SCALAR, reader.nextValue());
        assertInt(789, reader);
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(IonType.LIST, reader.getType());
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(IonType.LIST, reader.getType());
        assertStreamEnd(reader);
        reader.close();
    }

    private static void assertText(IonType type, String value, IonReaderBinaryIncrementalArbitraryDepth reader) throws IOException {
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(type, reader.getType());
        assertEquals(value, reader.stringValue());
    }

    private static void assertSymbol(String value, IonReaderBinaryIncrementalArbitraryDepth reader) throws IOException {
        assertText(IonType.SYMBOL, value, reader);
    }

    private static void assertString(String value, IonReaderBinaryIncrementalArbitraryDepth reader) throws IOException {
        assertText(IonType.STRING, value, reader);
    }

    @Test
    public void lstAppend() throws Exception {
        writerBuilder = IonBinaryWriterBuilder.standard().withLocalSymbolTableAppendEnabled();
        IonReaderBinaryIncrementalArbitraryDepth reader = readerFor(new WriterFunction() {
            @Override
            public void write(IonWriter writer) throws IOException {
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("foo");
                writer.addTypeAnnotation("uvw");
                writer.writeSymbol("abc");
                writer.setFieldName("bar");
                writer.setTypeAnnotations("qrs", "xyz");
                writer.writeSymbol("def");
                writer.stepOut();
                writer.flush();
                writer.writeSymbol("orange");
            }
        });

        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(IonType.STRUCT, reader.getType());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(IonType.SYMBOL, reader.getType());
        assertEquals("foo", reader.getFieldName());
        assertEquals(Collections.singletonList("uvw"), Arrays.asList(reader.getTypeAnnotations()));
        assertSymbol("abc", reader);
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(IonType.SYMBOL, reader.getType());
        assertEquals("bar", reader.getFieldName());
        assertEquals(Arrays.asList("qrs", "xyz"), Arrays.asList(reader.getTypeAnnotations()));
        assertSymbol("def", reader);
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        SymbolTable preAppend = reader.getSymbolTable();
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(IonType.SYMBOL, reader.getType());
        SymbolTable postAppend = reader.getSymbolTable();
        assertSymbol("orange", reader);
        assertNull(preAppend.find("orange"));
        assertNotNull(postAppend.find("orange"));
        assertStreamEnd(reader);
        reader.close();
    }

    @Test
    public void incrementalValue() throws Exception {
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        IonReaderBinaryIncrementalArbitraryDepth reader = new IonReaderBinaryIncrementalArbitraryDepth(
            STANDARD_READER_BUILDER,
            new IonBinaryLexerRefillableFromInputStream(pipe, STANDARD_READER_BUILDER.getBufferConfiguration())
        );
        byte[] bytes = toBinary("\"StringValueLong\"");
        boolean fillValue = false;
        for (int i = 0; i < bytes.length; i++) {
            if (i == _Private_IonConstants.BINARY_VERSION_MARKER_SIZE + 3) {
                assertEquals(START_SCALAR, reader.nextValue());
                fillValue = true;
            } else if (fillValue) {
                assertEquals(NEEDS_DATA, reader.fillValue());
            } else {
                assertEquals(NEEDS_DATA, reader.nextValue());
            }
            pipe.receive(bytes[i]);
        }
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(IonType.STRING, reader.getType());
        assertString("StringValueLong", reader);
        assertStreamEnd(reader);
        reader.close();
    }

    @Test
    public void incrementalSymbolTables() throws Exception {
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        ByteArrayOutputStream symbolTable = new ByteArrayOutputStream();
        _Private_IonRawWriter writer = IonBinaryWriterBuilder.standard().build(symbolTable)
            .asFacet(_Private_IonManagedWriter.class)
            .getRawWriter();
        symbolTable.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
        writer.setTypeAnnotationSymbols(3);
        writer.stepIn(IonType.STRUCT);
        writer.setFieldNameSymbol(7);
        writer.stepIn(IonType.LIST);
        writer.writeString("abcdefghijklmnopqrstuvwxyz");
        writer.writeString("def");
        writer.stepOut();
        writer.stepOut();
        writer.close();

        ByteArrayOutputStream firstValue = new ByteArrayOutputStream();
        writer = IonBinaryWriterBuilder.standard().build(firstValue)
            .asFacet(_Private_IonManagedWriter.class)
            .getRawWriter();
        writer.stepIn(IonType.STRUCT);
        writer.setTypeAnnotationSymbols(11);
        writer.setFieldNameSymbol(10);
        writer.writeString("foo");
        writer.stepOut();
        writer.close();

        ByteArrayOutputStream secondSymbolTable = new ByteArrayOutputStream();
        writer = IonBinaryWriterBuilder.standard().build(secondSymbolTable)
            .asFacet(_Private_IonManagedWriter.class)
            .getRawWriter();
        writer.setTypeAnnotationSymbols(3);
        writer.stepIn(IonType.STRUCT);
        writer.setFieldNameSymbol(6);
        writer.writeSymbolToken(3);
        writer.setFieldNameSymbol(7);
        writer.stepIn(IonType.LIST);
        writer.writeString("foo");
        writer.writeString("bar");
        writer.stepOut();
        writer.stepOut();
        writer.close();

        ByteArrayOutputStream secondValue = new ByteArrayOutputStream();
        writer = IonBinaryWriterBuilder.standard().build(secondValue)
            .asFacet(_Private_IonManagedWriter.class)
            .getRawWriter();
        writer.stepIn(IonType.STRUCT);
        writer.setFieldNameSymbol(10);
        writer.setTypeAnnotationSymbols(12, 13);
        writer.writeString("fairlyLongString");
        writer.stepOut();
        writer.close();

        IonReaderBinaryIncrementalArbitraryDepth reader = new IonReaderBinaryIncrementalArbitraryDepth(
            STANDARD_READER_BUILDER,
            new IonBinaryLexerRefillableFromInputStream(pipe, STANDARD_READER_BUILDER.getBufferConfiguration())
        );
        byte[] bytes = symbolTable.toByteArray();
        for (byte b : bytes) {
            assertEquals(NEEDS_DATA, reader.nextValue());
            pipe.receive(b);
        }
        bytes = firstValue.toByteArray();
        pipe.receive(bytes[0]); // This is the struct header
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        for (int i = 1; i < bytes.length - 4; i++) {
            assertEquals(NEEDS_DATA, reader.nextValue());
            pipe.receive(bytes[i]);
        }
        assertEquals(NEEDS_DATA, reader.nextValue());
        pipe.receive(bytes[bytes.length - 4]);
        assertEquals(START_SCALAR, reader.nextValue());
        for (int i = -3; i <= -1; i++) {
            assertEquals(NEEDS_DATA, reader.fillValue());
            pipe.receive(bytes[bytes.length + i]);
        }
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(IonType.STRING, reader.getType());
        assertEquals(Collections.singletonList("def"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals("abcdefghijklmnopqrstuvwxyz", reader.getFieldName());
        assertEquals("foo", reader.stringValue());
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(NEEDS_DATA, reader.nextValue());

        bytes = secondSymbolTable.toByteArray();
        for (byte b : bytes) {
            assertEquals(NEEDS_DATA, reader.nextValue());
            pipe.receive(b);
        }

        bytes = secondValue.toByteArray();
        pipe.receive(bytes[0]); // This is the struct type ID
        assertEquals(NEEDS_DATA, reader.nextValue());
        pipe.receive(bytes[1]); // This is the length byte
        pipe.receive(bytes[2]); // This is the first field SID
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        int finalStringLength = "fairlyLongString".length() + 2; // +2: 1 for the type ID, 1 for the length byte.
        int i = 3;
        for (; i < bytes.length - finalStringLength; i++) {
            assertEquals(NEEDS_DATA, reader.nextValue());
            pipe.receive(bytes[i]);
        }
        assertEquals(NEEDS_DATA, reader.nextValue());
        pipe.receive(bytes[i]); // This is "fairlyLongString"'s type ID
        assertEquals(NEEDS_DATA, reader.nextValue());
        pipe.receive(bytes[++i]); // This is "fairlyLongString"'s length byte
        pipe.receive(bytes[++i]); // This is "fairlyLongString"'s first field SID
        assertEquals(START_SCALAR, reader.nextValue());
        for (i += 1; i < bytes.length; i++) {
            assertEquals(NEEDS_DATA, reader.fillValue());
            pipe.receive(bytes[i]);
        }
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(IonType.STRING, reader.getType());
        assertEquals("fairlyLongString", reader.stringValue());
        assertEquals("abcdefghijklmnopqrstuvwxyz", reader.getFieldName());
        assertEquals(Arrays.asList("foo", "bar"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(NEEDS_DATA, reader.nextValue());
        reader.close();
    }
}
