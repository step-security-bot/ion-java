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
    void refill(int numberOfBytesToFill) throws IOException {
        int numberOfBytesFilled = inputStream.read(buffer, limit, numberOfBytesToFill);
        if (numberOfBytesFilled < 0) {
            return;
        }
        limit += numberOfBytesFilled;
    }

    @Override
    boolean seek(int numberOfBytes) throws IOException {
        int size = available();
        int unbufferedBytesToSkip = numberOfBytes - size;
        if (unbufferedBytesToSkip <= 0) {
            offset += numberOfBytes;
            bytesRequested = 0;
            instruction = Instruction.READY;
            return true;
        }
        offset = limit;
        int skipped = size;
        try {
            skipped += (int) inputStream.skip(unbufferedBytesToSkip);
        } catch (EOFException e) {
            // Certain InputStream implementations (e.g. GZIPInputStream) throw EOFException if more bytes are requested
            // to skip than are currently available (e.g. if a header or trailer is incomplete).
        }
        int shortfall = numberOfBytes - skipped;
        if (shortfall <= 0) {
            bytesRequested = 0;
            instruction = Instruction.READY;
            return true;
        }
        //remainingBytesRequested = shortfall;
        bytesRequested = numberOfBytes;
        instruction = Instruction.SEEK;
        return false;
    }

    @Override
    int readByteWithoutBuffering() throws IOException {
        return inputStream.read();
    }
}
