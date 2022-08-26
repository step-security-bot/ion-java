package com.amazon.ion.impl;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class RefillableBufferFromInputStream extends RefillableBuffer {

    private final InputStream inputStream;

    RefillableBufferFromInputStream(
        InputStream inputStream,
        int initialBufferSize,
        int maximumBufferSize
    ) {
        super(initialBufferSize, maximumBufferSize);
        this.inputStream = inputStream;
    }

    @Override
    void refill(long numberOfBytesToFill) throws IOException {
        int numberOfBytesFilled = inputStream.read(buffer, (int) limit, (int) numberOfBytesToFill);
        if (numberOfBytesFilled < 0) {
            return;
        }
        limit += numberOfBytesFilled;
    }

    @Override
    protected boolean carefulSeek(long numberOfBytes) throws IOException {
        long size = available();
        long unbufferedBytesToSkip = numberOfBytes - size;
        if (unbufferedBytesToSkip <= 0) {
            offset += numberOfBytes;
            bytesRequested = 0;
            state = State.READY;
            return true;
        }
        offset = limit;
        long skipped = size;
        try {
            skipped += inputStream.skip(unbufferedBytesToSkip);
        } catch (EOFException e) {
            // Certain InputStream implementations (e.g. GZIPInputStream) throw EOFException if more bytes are requested
            // to skip than are currently available (e.g. if a header or trailer is incomplete).
        }
        long shortfall = numberOfBytes - skipped;
        if (shortfall <= 0) {
            bytesRequested = 0;
            state = State.READY;
            return true;
        }
        //remainingBytesRequested = shortfall;
        //bytesRequested = numberOfBytes;
        bytesRequested = shortfall;
        state = State.SEEK;
        return false;
    }

    @Override
    int readByteWithoutBuffering() throws IOException {
        return inputStream.read();
    }
}
