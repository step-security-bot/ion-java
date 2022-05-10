package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonReaderIncremental;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static com.amazon.ion.BitUtils.bytes;
import static org.junit.Assert.assertEquals;

public class IonReaderLookaheadBufferArbitraryDepthTest {

    @Test
    public void basic() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        IonReaderLookaheadBufferArbitraryDepth buffer = new IonReaderLookaheadBufferArbitraryDepth(IonBufferConfiguration.Builder.standard().build(), in);
        assertEquals(IonReaderIncremental.Event.START_CONTAINER, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        assertEquals(IonReaderIncremental.Event.NEEDS_INSTRUCTION, buffer.next(IonReaderIncremental.Instruction.STEP_IN));
        assertEquals(IonReaderIncremental.Event.START_SCALAR, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        buffer.fillScalar();
        assertEquals(IonReaderIncremental.Event.END_CONTAINER, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        assertEquals(IonReaderIncremental.Event.NEEDS_INSTRUCTION, buffer.next(IonReaderIncremental.Instruction.STEP_OUT));
        assertEquals(IonReaderIncremental.Event.NEEDS_DATA, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
    }

    @Test
    public void basicNoFill() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        IonReaderLookaheadBufferArbitraryDepth buffer = new IonReaderLookaheadBufferArbitraryDepth(IonBufferConfiguration.Builder.standard().build(), in);
        assertEquals(IonReaderIncremental.Event.START_CONTAINER, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        assertEquals(IonReaderIncremental.Event.NEEDS_INSTRUCTION, buffer.next(IonReaderIncremental.Instruction.STEP_IN));
        assertEquals(IonReaderIncremental.Event.START_SCALAR, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        assertEquals(IonReaderIncremental.Event.END_CONTAINER, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        assertEquals(IonReaderIncremental.Event.NEEDS_INSTRUCTION, buffer.next(IonReaderIncremental.Instruction.STEP_OUT));
        assertEquals(IonReaderIncremental.Event.NEEDS_DATA, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
    }

    @Test
    public void basicStepOutEarly() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        IonReaderLookaheadBufferArbitraryDepth buffer = new IonReaderLookaheadBufferArbitraryDepth(IonBufferConfiguration.Builder.standard().build(), in);
        assertEquals(IonReaderIncremental.Event.START_CONTAINER, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        assertEquals(IonReaderIncremental.Event.NEEDS_INSTRUCTION, buffer.next(IonReaderIncremental.Instruction.STEP_IN));
        assertEquals(IonReaderIncremental.Event.START_SCALAR, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        assertEquals(IonReaderIncremental.Event.NEEDS_INSTRUCTION, buffer.next(IonReaderIncremental.Instruction.STEP_OUT));
        assertEquals(IonReaderIncremental.Event.NEEDS_DATA, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
    }

    @Test
    public void basicTopLevelSkip() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01));
        IonReaderLookaheadBufferArbitraryDepth buffer = new IonReaderLookaheadBufferArbitraryDepth(IonBufferConfiguration.Builder.standard().build(), in);
        assertEquals(IonReaderIncremental.Event.START_CONTAINER, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        assertEquals(IonReaderIncremental.Event.NEEDS_DATA, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
    }

    @Test
    public void basicTopLevelSkipThenConsume() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes(0xE0, 0x01, 0x00, 0xEA, 0xD3, 0x84, 0x21, 0x01, 0x21, 0x03));
        IonReaderLookaheadBufferArbitraryDepth buffer = new IonReaderLookaheadBufferArbitraryDepth(IonBufferConfiguration.Builder.standard().build(), in);
        assertEquals(IonReaderIncremental.Event.START_CONTAINER, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        assertEquals(IonReaderIncremental.Event.START_SCALAR, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
        buffer.fillScalar();
        assertEquals(IonReaderIncremental.Event.NEEDS_DATA, buffer.next(IonReaderIncremental.Instruction.NEXT_VALUE));
    }

    @Test
    public void nestedContainers() throws Exception {

    }
}
