package com.amazon.ion;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public interface IonCursor extends Closeable {
    enum Event {
        NEEDS_DATA,
        NEEDS_INSTRUCTION, // TODO consider adding START_ANNOTATION, allowing users to skip a value after reading an annotation
        START_SCALAR,
        VALUE_READY,
        START_CONTAINER,
        END_CONTAINER
    }

    Event nextValue() throws IOException;
    Event stepIntoContainer() throws IOException;
    Event stepOutOfContainer() throws IOException;
    Event fillValue() throws IOException;
    Event getCurrentEvent();
}
