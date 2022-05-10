package com.amazon.ion;

import java.io.InputStream;

public interface IonReaderIncremental {
    enum Event {
        NEEDS_DATA,
        NEEDS_INSTRUCTION,
        START_SCALAR,
        START_CONTAINER,
        END_CONTAINER
    }

    enum Instruction {
        NEXT_VALUE,
        STEP_IN,
        STEP_OUT
    }

    Event next(Instruction instruction);

    Event getCurrentEvent();

    // TODO more variants (byte[], ByteBuffer)
    void fill(InputStream inputStream);
}
