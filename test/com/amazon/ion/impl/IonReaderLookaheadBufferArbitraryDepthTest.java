package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonCursor;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.IonCursor.Event.END_CONTAINER;
import static com.amazon.ion.IonCursor.Event.NEEDS_DATA;
import static com.amazon.ion.IonCursor.Event.NEEDS_INSTRUCTION;
import static com.amazon.ion.IonCursor.Event.VALUE_READY;
import static com.amazon.ion.IonCursor.Event.START_CONTAINER;
import static com.amazon.ion.IonCursor.Event.START_SCALAR;
import static com.amazon.ion.IonCursor.Instruction.LOAD_VALUE;
import static com.amazon.ion.IonCursor.Instruction.NEXT_VALUE;
import static com.amazon.ion.IonCursor.Instruction.STEP_IN;
import static com.amazon.ion.IonCursor.Instruction.STEP_OUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IonReaderLookaheadBufferArbitraryDepthTest {

    private static final IonBufferConfiguration STANDARD_BUFFER_CONFIGURATION = IonBufferConfiguration.Builder.standard().build();

    private static IonBinaryLexerBase.Marker loadScalar(IonBinaryLexerRefillable buffer) {
        IonCursor.Event event = buffer.next(LOAD_VALUE);
        assertEquals(VALUE_READY, event);
        IonBinaryLexerBase.Marker marker = buffer.getValueMarker();
        assertNotNull(marker);
        return marker;
    }

    IonBinaryLexerRefillable buffer = null;
    int numberOfIvmsEncountered = 0;

    private final IonBinaryLexerBase.IvmNotificationConsumer countingIvmConsumer =
        new IonBinaryLexerBase.IvmNotificationConsumer() {

        @Override
        public void ivmEncountered(int majorVersion, int minorVersion) {
            numberOfIvmsEncountered++;
        }
    };

    @Before
    public void setup() {
        buffer = null;
        numberOfIvmsEncountered = 0;
    }

    private void initializeBuffer(byte[] bytes) {
        buffer = new IonBinaryLexerRefillable(
            STANDARD_BUFFER_CONFIGURATION,
            STANDARD_BUFFER_CONFIGURATION.getOversizedValueHandler(),
            countingIvmConsumer,
            new ByteArrayInputStream(bytes)
        );
    }

    @Test
    public void basicContainer() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_IN));
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        IonBinaryLexerBase.Marker marker = loadScalar(buffer);
        assertEquals(7, marker.startIndex);
        assertEquals(8, marker.endIndex);
        assertEquals(END_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_OUT));
        assertEquals(NEEDS_DATA, buffer.next(NEXT_VALUE));
    }

    @Test
    public void basicStrings() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0x83, 'f', 'o', 'o', 0x83, 'b', 'a', 'r'));
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        IonBinaryLexerBase.Marker marker = loadScalar(buffer);
        assertEquals(5, marker.startIndex);
        assertEquals(8, marker.endIndex);
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        marker = loadScalar(buffer);
        assertEquals(9, marker.startIndex);
        assertEquals(12, marker.endIndex);
        assertEquals(NEEDS_DATA, buffer.next(NEXT_VALUE));
    }

    @Test
    public void basicNoFill() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_IN));
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        assertEquals(END_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_OUT));
        assertEquals(NEEDS_DATA, buffer.next(NEXT_VALUE));
    }

    @Test
    public void basicStepOutEarly() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_IN));
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_OUT));
        assertEquals(NEEDS_DATA, buffer.next(NEXT_VALUE));
    }

    @Test
    public void basicTopLevelSkip() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_DATA, buffer.next(NEXT_VALUE));
    }

    @Test
    public void basicTopLevelSkipThenConsume() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01, 0x21, 0x03));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        IonBinaryLexerBase.Marker marker = loadScalar(buffer);
        assertEquals(9, marker.startIndex);
        assertEquals(10, marker.endIndex);
        assertEquals(NEEDS_DATA, buffer.next(NEXT_VALUE));
    }

    @Test
    public void nestedContainers() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD6, 0x83, 0xB1, 0x40, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_IN));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_IN));
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_OUT));
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        IonBinaryLexerBase.Marker marker = loadScalar(buffer);
        assertEquals(10, marker.startIndex);
        assertEquals(11, marker.endIndex);
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_OUT));
        assertEquals(NEEDS_DATA, buffer.next(NEXT_VALUE));
    }

    @Test
    public void fillContainerAtDepth0() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD6, 0x83, 0xB1, 0x40, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(VALUE_READY, buffer.next(LOAD_VALUE));
        IonBinaryLexerBase.Marker marker = buffer.getValueMarker();
        assertEquals(5, marker.startIndex);
        assertEquals(11, marker.endIndex);

        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_IN));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_IN));
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_OUT));
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        marker = loadScalar(buffer);
        assertEquals(10, marker.startIndex);
        assertEquals(11, marker.endIndex);
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_OUT));
        assertEquals(NEEDS_DATA, buffer.next(NEXT_VALUE));
    }

    @Test
    public void fillContainerAtDepth1() throws Exception {
        initializeBuffer(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD6, 0x83, 0xB1, 0x40, 0x84, 0x21, 0x01));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_IN));
        assertEquals(START_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(VALUE_READY, buffer.next(LOAD_VALUE));
        IonBinaryLexerBase.Marker marker = buffer.getValueMarker();
        assertEquals(7, marker.startIndex);
        assertEquals(8, marker.endIndex);

        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_IN));
        assertEquals(START_SCALAR, buffer.next(NEXT_VALUE));
        assertEquals(END_CONTAINER, buffer.next(NEXT_VALUE));
        assertEquals(NEEDS_INSTRUCTION, buffer.next(STEP_OUT));
    }
}
