package com.amazon.ion.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class AbstractBuffer {

    protected enum State {
        FILL,
        SEEK,
        READY,
        TERMINATED
    }

    private interface SeekFunction {
        boolean seek(long numberOfBytes) throws IOException;
    }

    private interface FillAtFunction {
        boolean fillAt(long index, long numberOfBytes) throws Exception;
    }

    private final SeekFunction quickSeekFunction = new SeekFunction() {
        @Override
        public boolean seek(long numberOfBytes) {
            return quickSeek(numberOfBytes);
        }
    };

    private final SeekFunction carefulSeekFunction = new SeekFunction() {
        @Override
        public boolean seek(long numberOfBytes) throws IOException {
            return carefulSeek(numberOfBytes);
        }
    };

    private final FillAtFunction quickFillAtFunction = new FillAtFunction() {
        @Override
        public boolean fillAt(long index, long numberOfBytes) {
            return quickFillAt(index, numberOfBytes);
        }
    };

    private final FillAtFunction carefulFillAtFunction = new FillAtFunction() {
        @Override
        public boolean fillAt(long index, long numberOfBytes) throws Exception {
            return carefulFillAt(index, numberOfBytes);
        }
    };

    private SeekFunction currentSeekFunction = carefulSeekFunction;

    private FillAtFunction currentFillAtFunction = carefulFillAtFunction;

    private boolean isQuick = false;

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

    // true if already quick
    final boolean quick() {
        if (isQuick) {
            return true;
        }
        currentSeekFunction = quickSeekFunction;
        currentFillAtFunction = quickFillAtFunction;
        isQuick = true;
        return false;
    }

    final void careful() {
        currentSeekFunction = carefulSeekFunction;
        currentFillAtFunction = carefulFillAtFunction;
        isQuick = false;
    }

    final boolean fill(long numberOfBytes) throws Exception {
        return fillAt(offset, numberOfBytes);
    }

    protected abstract boolean carefulFillAt(long index, long numberOfBytes) throws Exception;

    protected abstract boolean carefulSeek(long numberOfBytes) throws IOException;

    final boolean fillAt(long index, long numberOfBytes) throws Exception {
        return currentFillAtFunction.fillAt(index, numberOfBytes);
    }

    final boolean seek(long numberOfBytes) throws IOException {
        return currentSeekFunction.seek(numberOfBytes);
    }

    final boolean seekTo(long index) throws IOException {
        return seek(index - offset);
    }

    private boolean quickFillAt(long index, long numberOfBytes) {
        state = State.READY;
        return true;
    }

    private boolean quickSeek(long numberOfBytes) {
        offset += numberOfBytes;
        state = State.READY;
        return true;
    }

    boolean makeReady() throws Exception {
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

    void terminate() {
        state = State.TERMINATED;
    }

    boolean isTerminated() {
        return state == State.TERMINATED;
    }

}
