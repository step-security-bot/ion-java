package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonBufferConfiguration;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

final class RefillableBufferFromInputStream extends RefillableBuffer {

    /**
     * The standard {@link IonBufferConfiguration}. This will be used unless the user chooses custom settings.
     */
    private static final IonBufferConfiguration STANDARD_BUFFER_CONFIGURATION =
        IonBufferConfiguration.Builder.standard().build();

    /**
     * @param value a non-negative number.
     * @return the exponent of the next power of two greater than the given number.
     */
    private static int logBase2(int value) {
        return 32 - Integer.numberOfLeadingZeros(value == 0 ? 0 : value - 1);
    }

    /**
     * Cache of configurations for fixed-sized streams. FIXED_SIZE_CONFIGURATIONS[i] returns a configuration with
     * buffer size max(8, 2^i). Retrieve a configuration large enough for a given size using
     * FIXED_SIZE_CONFIGURATIONS(logBase2(size)). Only supports sizes less than or equal to
     * STANDARD_BUFFER_CONFIGURATION.getInitialBufferSize().
     */
    private static final BufferConfiguration<?>[] FIXED_SIZE_CONFIGURATIONS;

    static {
        int maxBufferSizeExponent = logBase2(STANDARD_BUFFER_CONFIGURATION.getInitialBufferSize());
        FIXED_SIZE_CONFIGURATIONS = new IonBufferConfiguration[maxBufferSizeExponent + 1];
        for (int i = 0; i <= maxBufferSizeExponent; i++) {
            // Create a buffer configuration for buffers of size 2^i. The minimum size is 8: the smallest power of two
            // larger than the minimum buffer size allowed.
            int size = Math.max(8, (int) Math.pow(2, i));
            FIXED_SIZE_CONFIGURATIONS[i] = IonBufferConfiguration.Builder.from(STANDARD_BUFFER_CONFIGURATION)
                .withInitialBufferSize(size)
                .withMaximumBufferSize(size)
                .build();
        }
    }

    private static BufferConfiguration<?> configuration(InputStream inputStream, BufferConfiguration<?> configuration) {
        if (configuration == null) {
            if (inputStream instanceof ByteArrayInputStream) {
                // ByteArrayInputStreams are fixed-size streams. Clamp the reader's internal buffer size at the size of
                // the stream to avoid wastefully allocating extra space that will never be needed. It is still
                // preferable for the user to manually specify the buffer size if it's less than the default, as doing
                // so allows this branch to be skipped.
                int fixedBufferSize;
                try {
                    fixedBufferSize = inputStream.available();
                } catch (IOException e) {
                    // ByteArrayInputStream.available() does not throw.
                    throw new IllegalStateException(e);
                }
                if (STANDARD_BUFFER_CONFIGURATION.getInitialBufferSize() > fixedBufferSize) {
                    configuration = FIXED_SIZE_CONFIGURATIONS[logBase2(fixedBufferSize)];
                } else {
                    configuration = STANDARD_BUFFER_CONFIGURATION;
                }
            } else {
                configuration = STANDARD_BUFFER_CONFIGURATION;
            }
        }
        return configuration;
    }

    private final InputStream inputStream;

    RefillableBufferFromInputStream(
        InputStream inputStream,
        BufferConfiguration<?> configuration
    ) {
        super(configuration(inputStream, configuration));
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
    protected boolean seek(long numberOfBytes) throws IOException {
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

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
