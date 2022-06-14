package com.amazon.ion;

import java.io.InputStream;

// TODO consider a different one for raw
public interface IonReaderIncremental {
    enum Event {
        NEEDS_DATA,
        NEEDS_INSTRUCTION, // TODO consider adding START_ANNOTATION, allowing users to skip a value after reading an annotation
        START_SCALAR,
        SCALAR_READY,
        START_CONTAINER,
        END_CONTAINER
    }

    enum Instruction {
        NEXT_VALUE,
        LOAD_SCALAR,
        STEP_IN,
        STEP_OUT
    }

    Event next(Instruction instruction);

    Event getCurrentEvent();

    // TODO more variants (byte[], ByteBuffer)
    void fill(InputStream inputStream);
}
