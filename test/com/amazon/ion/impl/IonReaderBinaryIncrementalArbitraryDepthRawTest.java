package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.IonCursor.Event.END_CONTAINER;
import static com.amazon.ion.IonCursor.Event.NEEDS_DATA;
import static com.amazon.ion.IonCursor.Event.NEEDS_INSTRUCTION;
import static com.amazon.ion.IonCursor.Event.START_CONTAINER;
import static com.amazon.ion.IonCursor.Event.START_SCALAR;
import static com.amazon.ion.IonCursor.Event.VALUE_READY;
import static org.junit.Assert.assertEquals;

public class IonReaderBinaryIncrementalArbitraryDepthRawTest {
    private static final IonBufferConfiguration STANDARD_BUFFER_CONFIGURATION = IonBufferConfiguration.Builder.standard().build();

    IonReaderBinaryIncrementalArbitraryDepthRaw reader = null;
    int numberOfIvmsEncountered = 0;

    private final IonBinaryLexerRefillable.IvmNotificationConsumer countingIvmConsumer =
        new IonBinaryLexerRefillable.IvmNotificationConsumer() {

            @Override
            public void ivmEncountered(int majorVersion, int minorVersion) {
                numberOfIvmsEncountered++;
            }
        };

    @Before
    public void setup() {
        reader = null;
        numberOfIvmsEncountered = 0;
    }

    private void initializeBuffer(byte[] bytes) {
        // TODO parameterize to try both InputStream and bytes
        reader = new IonReaderBinaryIncrementalArbitraryDepthRaw(
            new IonBinaryLexerRefillable(
                new RefillableBufferFromInputStream(new ByteArrayInputStream(bytes), STANDARD_BUFFER_CONFIGURATION)
            )
        );
        reader.lexer.registerIvmNotificationConsumer(countingIvmConsumer);
        ((IonBinaryLexerRefillable) reader.lexer).registerOversizedValueHandler(
            STANDARD_BUFFER_CONFIGURATION.getOversizedValueHandler()
        );
    }

    @Test
    public void basicContainer() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIn());
        assertEquals(START_SCALAR, reader.next());
        assertEquals(4, reader.getFieldId());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(1, reader.intValue());
        assertEquals(END_CONTAINER, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOut());
        assertEquals(NEEDS_DATA, reader.next());
    }

    @Test
    public void basicStrings() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0x83, 'f', 'o', 'o', 0x83, 'b', 'a', 'r'));
        assertEquals(START_SCALAR, reader.next());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals("foo", reader.stringValue());
        assertEquals(START_SCALAR, reader.next());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals("bar", reader.stringValue());
        assertEquals(NEEDS_DATA, reader.next());
    }

    @Test
    public void basicNoFill() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIn());
        assertEquals(START_SCALAR, reader.next());
        assertEquals(4, reader.getFieldId());
        assertEquals(END_CONTAINER, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOut());
        assertEquals(NEEDS_DATA, reader.next());
    }

    @Test
    public void basicStepOutEarly() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIn());
        assertEquals(START_SCALAR, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOut());
        assertEquals(-1, reader.getFieldId());
        assertEquals(NEEDS_DATA, reader.next());
    }

    @Test
    public void basicTopLevelSkip() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(NEEDS_DATA, reader.next());
    }

    @Test
    public void basicTopLevelSkipThenConsume() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01, 0x21, 0x03));
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(START_SCALAR, reader.next());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(3, reader.intValue());
        assertEquals(NEEDS_DATA, reader.next());
    }

    @Test
    public void nestedContainers() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD6, 0x83, 0xB1, 0x40, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIn());
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIn());
        assertEquals(START_SCALAR, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOut());
        assertEquals(START_SCALAR, reader.next());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(1, reader.intValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOut());
        assertEquals(NEEDS_DATA, reader.next());
    }

    @Test
    public void fillContainerAtDepth0() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD6, 0x83, 0xB1, 0x40, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(VALUE_READY, reader.fillValue());

        assertEquals(NEEDS_INSTRUCTION, reader.stepIn());
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIn());
        assertEquals(START_SCALAR, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOut());
        assertEquals(START_SCALAR, reader.next());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(1, reader.intValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOut());
        assertEquals(NEEDS_DATA, reader.next());
    }

    @Test
    public void fillContainerAtDepth1() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD6, 0x83, 0xB1, 0x40, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIn());
        assertEquals(START_CONTAINER, reader.next());
        assertEquals(VALUE_READY, reader.fillValue());

        assertEquals(NEEDS_INSTRUCTION, reader.stepIn());
        assertEquals(START_SCALAR, reader.next());
        assertEquals(END_CONTAINER, reader.next());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOut());
    }
}
