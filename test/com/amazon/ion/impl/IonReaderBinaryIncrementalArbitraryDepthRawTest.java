package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.IonReaderIncremental.Event.END_CONTAINER;
import static com.amazon.ion.IonReaderIncremental.Event.NEEDS_DATA;
import static com.amazon.ion.IonReaderIncremental.Event.NEEDS_INSTRUCTION;
import static com.amazon.ion.IonReaderIncremental.Event.START_CONTAINER;
import static com.amazon.ion.IonReaderIncremental.Event.START_SCALAR;
import static com.amazon.ion.IonReaderIncremental.Event.VALUE_READY;
import static com.amazon.ion.IonReaderIncremental.Instruction.LOAD_VALUE;
import static com.amazon.ion.IonReaderIncremental.Instruction.NEXT_VALUE;
import static com.amazon.ion.IonReaderIncremental.Instruction.STEP_IN;
import static com.amazon.ion.IonReaderIncremental.Instruction.STEP_OUT;
import static org.junit.Assert.assertEquals;

public class IonReaderBinaryIncrementalArbitraryDepthRawTest {
    private static final IonBufferConfiguration STANDARD_BUFFER_CONFIGURATION = IonBufferConfiguration.Builder.standard().build();

    IonReaderBinaryIncrementalArbitraryDepthRaw reader = null;
    int numberOfIvmsEncountered = 0;

    private final IonReaderLookaheadBufferArbitraryDepth.IvmNotificationConsumer countingIvmConsumer =
        new IonReaderLookaheadBufferArbitraryDepth.IvmNotificationConsumer() {

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
        reader = new IonReaderBinaryIncrementalArbitraryDepthRaw(
            STANDARD_BUFFER_CONFIGURATION,
            countingIvmConsumer,
            new ByteArrayInputStream(bytes)
        );
    }

    @Test
    public void basicContainer() {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_IN));
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(4, reader.getFieldId());
        assertEquals(VALUE_READY, reader.next(LOAD_VALUE));
        assertEquals(1, reader.intValue());
        assertEquals(END_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_OUT));
        assertEquals(NEEDS_DATA, reader.next(NEXT_VALUE));
    }

    @Test
    public void basicStrings() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0x83, 'f', 'o', 'o', 0x83, 'b', 'a', 'r'));
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(VALUE_READY, reader.next(LOAD_VALUE));
        assertEquals("foo", reader.stringValue());
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(VALUE_READY, reader.next(LOAD_VALUE));
        assertEquals("bar", reader.stringValue());
        assertEquals(NEEDS_DATA, reader.next(NEXT_VALUE));
    }

    @Test
    public void basicNoFill() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_IN));
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(4, reader.getFieldId());
        assertEquals(END_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_OUT));
        assertEquals(NEEDS_DATA, reader.next(NEXT_VALUE));
    }

    @Test
    public void basicStepOutEarly() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_IN));
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_OUT));
        assertEquals(-1, reader.getFieldId());
        assertEquals(NEEDS_DATA, reader.next(NEXT_VALUE));
    }

    @Test
    public void basicTopLevelSkip() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_DATA, reader.next(NEXT_VALUE));
    }

    @Test
    public void basicTopLevelSkipThenConsume() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01, 0x21, 0x03));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(VALUE_READY, reader.next(LOAD_VALUE));
        assertEquals(3, reader.intValue());
        assertEquals(NEEDS_DATA, reader.next(NEXT_VALUE));
    }

    @Test
    public void nestedContainers() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD6, 0x83, 0xB1, 0x40, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_IN));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_IN));
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_OUT));
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(VALUE_READY, reader.next(LOAD_VALUE));
        assertEquals(1, reader.intValue());
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_OUT));
        assertEquals(NEEDS_DATA, reader.next(NEXT_VALUE));
    }

    @Test
    public void fillContainerAtDepth0() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD6, 0x83, 0xB1, 0x40, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(VALUE_READY, reader.next(LOAD_VALUE));

        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_IN));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_IN));
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_OUT));
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(VALUE_READY, reader.next(LOAD_VALUE));
        assertEquals(1, reader.intValue());
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_OUT));
        assertEquals(NEEDS_DATA, reader.next(NEXT_VALUE));
    }

    @Test
    public void fillContainerAtDepth1() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD6, 0x83, 0xB1, 0x40, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_IN));
        assertEquals(START_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(VALUE_READY, reader.next(LOAD_VALUE));

        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_IN));
        assertEquals(START_SCALAR, reader.next(NEXT_VALUE));
        assertEquals(END_CONTAINER, reader.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, reader.next(STEP_OUT));
    }
}
