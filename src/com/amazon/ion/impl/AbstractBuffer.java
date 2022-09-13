package com.amazon.ion.impl;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

// TODO get rid of this, move logic to LexerBase
abstract class AbstractBuffer implements Closeable {

    protected enum State {
        FILL,
        SEEK,
        READY,
        TERMINATED
    }

    private interface SeekToFunction {
        boolean seekTo(long index) throws IOException;
    }

    private interface FillAtFunction {
        boolean fillAt(long index, long numberOfBytes) throws IOException;
    }

    private final SeekToFunction quickSeekToFunction = new SeekToFunction() {
        @Override
        public boolean seekTo(long index) {
            quickSeekTo(index);
            return true;
        }
    };

    private final SeekToFunction carefulSeekToFunction = new SeekToFunction() {
        @Override
        public boolean seekTo(long index) throws IOException {
            return seek(index - offset);
        }
    };

    private static final FillAtFunction QUICK_FILL_AT_FUNCTION = new FillAtFunction() {
        @Override
        public boolean fillAt(long index, long numberOfBytes) {
            return true;
        }
    };

    private final FillAtFunction carefulFillAtFunction = new FillAtFunction() {
        @Override
        public boolean fillAt(long index, long numberOfBytes) throws IOException {
            return carefulFillAt(index, numberOfBytes);
        }
    };

    private SeekToFunction currentSeekToFunction = carefulSeekToFunction;

    private FillAtFunction currentFillAtFunction = carefulFillAtFunction;

    /**
     * Mask to isolate a single byte.
     */
    static final int SINGLE_BYTE_MASK = 0xFF;

    /**
     * The index of the next byte in the buffer that is available to be read. Always less than or equal to `limit`.
     */
    long offset = 0;

    /**
     * The index at which the next byte received will be written. Always greater than or equal to `offset`.
     */
    long limit = 0;

    long capacity;

    ByteBuffer byteBuffer;

    State state = State.READY;

    long bytesRequested = 0;

    abstract int peek(long index);

    // TODO abstraction?
    ByteBuffer getByteBuffer(long startIndex, long endIndex) {
        // Setting the limit to the capacity first is required because setting the position will fail if the new
        // position is outside the limit.
        byteBuffer.limit((int) capacity);
        byteBuffer.position((int) startIndex);
        byteBuffer.limit((int) endIndex);
        return byteBuffer;
    }

    final long available() {
        return availableAt(offset);
    }

    final long availableAt(long index) {
        return limit - index;
    }

    abstract void copyBytes(long position, byte[] destination, int destinationOffset, int length);

    final void quick() {
        currentSeekToFunction = quickSeekToFunction;
        currentFillAtFunction = QUICK_FILL_AT_FUNCTION;
    }

    final void careful() {
        currentSeekToFunction = carefulSeekToFunction;
        currentFillAtFunction = carefulFillAtFunction;
    }

    final boolean fill(long numberOfBytes) throws IOException {
        return fillAt(offset, numberOfBytes);
    }

    protected abstract boolean carefulFillAt(long index, long numberOfBytes) throws IOException;

    final boolean fillAt(long index, long numberOfBytes) throws IOException {
        return currentFillAtFunction.fillAt(index, numberOfBytes);
    }

    abstract boolean seek(long numberOfBytes) throws IOException;

    final void quickSeekTo(long index) {
        offset = index;
    }

    final boolean seekTo(long index) throws IOException {
        return currentSeekToFunction.seekTo(index);
    }

    boolean makeReady() throws IOException {
        switch (state) {
            case READY:
                return true;
            case SEEK:
                return seek(bytesRequested);
            case FILL:
                return fill(bytesRequested);
            case TERMINATED:
                return false;
            default:
                throw new IllegalStateException();
        }
    }

    long getOffset() {
        return offset;
    }

    boolean isReady() {
        return state == State.READY;
    }

    boolean isAwaitingMoreData() {
        // TODO doesn't feel quite right
        return state == State.SEEK || bytesRequested > 1;
    }
}
