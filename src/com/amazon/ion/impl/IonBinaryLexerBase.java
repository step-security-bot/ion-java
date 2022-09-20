package com.amazon.ion.impl;

import static com.amazon.ion.IonCursor.Event;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonType;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

// TODO removed 'implements IonCursor' as a performance experiment. Try adding back.
class IonBinaryLexerBase implements IonCursor {

    protected static final int LOWER_SEVEN_BITS_BITMASK = 0x7F;
    protected static final int HIGHEST_BIT_BITMASK = 0x80;
    protected static final int VALUE_BITS_PER_VARUINT_BYTE = 7;
    // Note: because long is a signed type, Long.MAX_VALUE is represented in Long.SIZE - 1 bits.
    protected static final int MAXIMUM_SUPPORTED_VAR_UINT_BYTES = (Long.SIZE - 1) / VALUE_BITS_PER_VARUINT_BYTE;
    protected static final int IVM_START_BYTE = 0xE0;
    protected static final int IVM_FINAL_BYTE = 0xEA;
    protected static final int IVM_REMAINING_LENGTH = 3; // Length of the IVM after the first byte.

    /**
     * Mask to isolate a single byte.
     */
    static final int SINGLE_BYTE_MASK = 0xFF;

    private static final BufferConfiguration.DataHandler NO_OP_DATA_HANDLER = new BufferConfiguration.DataHandler() {
        @Override
        public void onData(long numberOfBytes) {
            // Do nothing.
        }
    };

    /**
     * Holds the start and end indices of a slice of the buffer.
     */
    static class Marker {
        /**
         * Index of the first byte in the slice.
         */
        long startIndex;

        /**
         * Index of the first byte after the end of the slice.
         */
        long endIndex;

        /**
         * @param startIndex index of the first byte in the slice.
         * @param length the number of bytes in the slice.
         */
        private Marker(final int startIndex, final int length) {
            this.startIndex = startIndex;
            this.endIndex = startIndex + length;
        }
    }

    interface IvmNotificationConsumer {
        void ivmEncountered(int majorVersion, int minorVersion);
    }

    /**
     * Holds the information that the binary reader must keep track of for containers at any depth.
     */
    protected static class ContainerInfo {

        /**
         * The container's type.
         */
        IonType type;

        /**
         * The byte position of the end of the container.
         */
        long endIndex;
    }

    // Constructs ContainerInfo instances.
    private static final _Private_RecyclingStack.ElementFactory<ContainerInfo> CONTAINER_INFO_FACTORY =
        new _Private_RecyclingStack.ElementFactory<ContainerInfo>() {

            @Override
            public ContainerInfo newElement() {
                return new ContainerInfo();
            }
        };

    // Initial capacity of the stack used to hold ContainerInfo. Each additional level of nesting in the data requires
    // a new ContainerInfo. Depths greater than 8 will be rare.
    private static final int CONTAINER_STACK_INITIAL_CAPACITY = 8;

    /**
     * Stack to hold container info. Stepping into a container results in a push; stepping out results in a pop.
     */
    protected final _Private_RecyclingStack<ContainerInfo> containerStack;

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

    private final InputStream inputStream;

    private final boolean isRefillable;

    /**
     * The handler that will be notified when data is processed.
     */
    protected final BufferConfiguration.DataHandler dataHandler;

    /**
     * Marker for the sequence of annotation symbol IDs on the current value. If there are no annotations on the
     * current value, the startIndex will be negative.
     */
    protected final Marker annotationSidsMarker = new Marker(-1, 0);

    protected final Marker valueMarker = new Marker(-1, 0);

    private IvmNotificationConsumer ivmConsumer;

    protected IonCursor.Event event = IonCursor.Event.NEEDS_DATA;

    protected byte[] buffer;

    // The major version of the Ion encoding currently being read.
    private int majorVersion = -1;

    // The minor version of the Ion encoding currently being read.
    private int minorVersion = 0;

    /**
     * The type ID byte of the current value.
     */
    protected IonTypeID valueTid;

    protected int fieldSid = -1;
    
    protected long checkpoint;

    protected long peekIndex;

    IonBinaryLexerBase(
        final IonBufferConfiguration configuration,
        byte[] buffer,
        int offset,
        int length
    ) {
        this.dataHandler = (configuration == null || configuration.getDataHandler() == null)
            ? NO_OP_DATA_HANDLER
            : configuration.getDataHandler();
        containerStack = new _Private_RecyclingStack<ContainerInfo>(
            CONTAINER_STACK_INITIAL_CAPACITY,
            CONTAINER_INFO_FACTORY
        );
        peekIndex = offset;
        checkpoint = peekIndex;

        this.configuration = configuration;
        this.buffer = buffer;
        this.offset = offset;
        this.limit = offset + length;
        this.capacity = limit;
        initialBufferSize = (int) capacity;
        maximumBufferSize = (int) capacity;
        pageSize = initialBufferSize;
        byteBuffer = ByteBuffer.wrap(buffer, offset, length);
        inputStream = null;
        isRefillable = false;
    }

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
    private static final IonBufferConfiguration[] FIXED_SIZE_CONFIGURATIONS;

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

    private static IonBufferConfiguration validate(IonBufferConfiguration configuration) {
        if (configuration.getInitialBufferSize() < 1) {
            throw new IllegalArgumentException("Initial buffer size must be at least 1.");
        }
        if (configuration.getMaximumBufferSize() < configuration.getInitialBufferSize()) {
            throw new IllegalArgumentException("Maximum buffer size cannot be less than the initial buffer size.");
        }
        return configuration;
    }

    final IonBufferConfiguration configuration;
    
    /**
     * The initial size of the buffer and the number of bytes by which the size of the buffer will increase
     * each time it grows, unless it must grow by a smaller amount to fit within 'maximumBufferSize'.
     */
    protected final int initialBufferSize;

    /**
     * The maximum size of the buffer. If the user attempts to buffer more bytes than this, an exception will be raised.
     */
    protected final int maximumBufferSize;

    private BufferConfiguration.OversizedValueHandler oversizedValueHandler;

    /**
     * The number of bytes to attempt to buffer each time more bytes are required.
     */
    private final int pageSize;

    private boolean isSkippingCurrentValue = false;

    private int individualBytesSkippedWithoutBuffering = 0;

    private CheckpointLocation checkpointLocation = CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID;

    private final IonCursor careful = new Careful();

    private final IonCursor quick = new Quick();

    private IonCursor current;

    protected State state = State.READY;

    protected long bytesRequested = 0;

    IonBinaryLexerBase(
        IonBufferConfiguration configuration,
        InputStream inputStream
    ) {
        this.dataHandler = (configuration == null || configuration.getDataHandler() == null)
            ? NO_OP_DATA_HANDLER
            : configuration.getDataHandler();
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
        validate(configuration);
        containerStack = new _Private_RecyclingStack<ContainerInfo>(
            CONTAINER_STACK_INITIAL_CAPACITY,
            CONTAINER_INFO_FACTORY
        );
        peekIndex = offset;
        checkpoint = peekIndex;

        this.configuration = configuration;
        this.buffer = new byte[configuration.getInitialBufferSize()];
        this.offset = 0;
        this.limit = 0;
        this.capacity = configuration.getInitialBufferSize();
        this.initialBufferSize = configuration.getInitialBufferSize();
        this.maximumBufferSize = configuration.getMaximumBufferSize();
        pageSize = configuration.getInitialBufferSize();
        current = careful;
        byteBuffer = ByteBuffer.wrap(buffer, 0, configuration.getInitialBufferSize());
        this.inputStream = inputStream;
        isRefillable = true;
    }

    void registerIvmNotificationConsumer(IvmNotificationConsumer ivmConsumer) {
        this.ivmConsumer = ivmConsumer;
    }

    // TODO is it possible to just put this in the constructor?
    void registerOversizedValueHandler(BufferConfiguration.OversizedValueHandler oversizedValueHandler) {
        this.oversizedValueHandler = oversizedValueHandler;
    }

    protected void setValueMarker(long valueLength, boolean isAnnotated) {
        if (isSkippingCurrentValue) {
            // If the value is being skipped, not all of its bytes will be buffered, so start/end indexes will not
            // align to the expected values. This is fine, because the value will not be accessed.
            return;
        }
        long endIndex = peekIndex + valueLength;
        ContainerInfo parent = containerStack.peek();
        if (parent != null && endIndex > parent.endIndex) {
            throw new IonException("Value exceeds the length of its parent container.");
        }
        if (isAnnotated && endIndex != valueMarker.endIndex) {
            // valueMarker.endIndex refers to the end of the annotation wrapper.
            throw new IonException("Annotation wrapper length does not match the length of the wrapped value.");
        }
        valueMarker.startIndex = peekIndex;
        valueMarker.endIndex = endIndex;
    }

    protected boolean checkContainerEnd() {
        ContainerInfo parent = containerStack.peek();
        if (parent == null || parent.endIndex > peekIndex) {
            return false;
        }
        if (parent.endIndex == peekIndex) {
            event = Event.END_CONTAINER;
            valueTid = null;
            fieldSid = -1;
            return true;
        }
        throw new IonException("Contained values overflowed the parent container length.");
    }

    protected void reset() {
        valueMarker.startIndex = -1;
        valueMarker.endIndex = -1;
        annotationSidsMarker.startIndex = -1;
        annotationSidsMarker.endIndex = -1;
        fieldSid = -1;
    }

    // TODO test that this is called when necessary in quick/fixed mode
    protected void reportConsumedData(long numberOfBytesToReport) {
        dataHandler.onData(numberOfBytesToReport);
    }

    protected void parseIvm() {
        majorVersion = buffer[(int) (peekIndex++)];
        minorVersion = buffer[(int) (peekIndex++)];
        if ((buffer[(int) (peekIndex++)] & SINGLE_BYTE_MASK) != IVM_FINAL_BYTE) {
            throw new IonException("Invalid Ion version marker.");
        }
        if (majorVersion != 1 || minorVersion != 0) {
            throw new IonException(String.format("Unsupported Ion version: %d.%d", majorVersion, minorVersion));
        }
        ivmConsumer.ivmEncountered(majorVersion, minorVersion);
    }

    protected void prohibitEmptyOrderedStruct() {
        if (valueTid.type == IonType.STRUCT &&
            valueTid.lowerNibble == IonTypeID.ORDERED_STRUCT_NIBBLE &&
            valueMarker.endIndex == peekIndex
        ) {
            throw new IonException("Ordered struct must not be empty.");
        }
    }

    private boolean parseAnnotationWrapperHeader(IonTypeID valueTid) {
        long valueLength;
        if (valueTid.variableLength) {
            valueLength = readVarUInt();
        } else {
            valueLength = valueTid.length;
        }
        setValueMarker(valueLength, false);
        if (valueMarker.endIndex > limit) {
            return true;
        }
        int annotationsLength = (int) readVarUInt();
        annotationSidsMarker.startIndex = peekIndex;
        annotationSidsMarker.endIndex = annotationSidsMarker.startIndex + annotationsLength;
        peekIndex = annotationSidsMarker.endIndex;
        if (peekIndex >= valueMarker.endIndex) {
            throw new IonException("Annotation wrapper must wrap a value.");
        }
        return false;
    }

    private boolean parseValueHeader(IonTypeID valueTid, boolean isAnnotated) {
        long valueLength;
        if (valueTid.variableLength) {
            valueLength = readVarUInt();
        } else {
            valueLength = valueTid.length;
        }
        if (IonType.isContainer(valueTid.type)) {
            event = Event.START_CONTAINER;
        } else if (valueTid.isNopPad) {
            if (isAnnotated) {
                throw new IonException(
                    "Invalid annotation wrapper: NOP pad may not occur inside an annotation wrapper."
                );
            }
            long destination = peekIndex + valueLength;
            if (destination > limit) {
                throw new IonException("Invalid NOP pad.");
            }
            peekIndex += valueLength;
            valueLength = 0;
            checkContainerEnd();
        } else {
            event = Event.START_SCALAR;
        }
        setValueMarker(valueLength, isAnnotated);
        if (valueMarker.endIndex > limit) {
            event = Event.NEEDS_DATA;
            return true;
        }
        return false;
    }

    /**
     * Reads the type ID byte.
     *
     * @param isAnnotated true if this type ID is on a value within an annotation wrapper; false if it is not.
     */
    private boolean parseTypeID(final int typeIdByte, final boolean isAnnotated) {
        IonTypeID valueTid = IonTypeID.TYPE_IDS[typeIdByte];
        if (!valueTid.isValid) {
            throw new IonException("Invalid type ID.");
        } else if (valueTid.type == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
            // Annotation.
            if (isAnnotated) {
                throw new IonException("Nested annotation wrappers are invalid.");
            }
            if (parseAnnotationWrapperHeader(valueTid)) {
                return true;
            }
            return parseTypeID(buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK, true);
        } else {
            if (parseValueHeader(valueTid, isAnnotated)) {
                return true;
            }
        }
        IonBinaryLexerBase.this.valueTid = valueTid;
        if (event == Event.START_CONTAINER) {
            prohibitEmptyOrderedStruct();
            return true;
        }
        return event == Event.START_SCALAR;
    }


    /**
     * Reads a VarUInt. NOTE: the VarUInt must fit in a `long`. This is not a true limitation, as IonJava requires
     * VarUInts to fit in an `int`.
     *
     */
    private long readVarUInt() {
        int currentByte = 0;
        long result = 0;
        while ((currentByte & HIGHEST_BIT_BITMASK) == 0) {
            currentByte = buffer[(int) (peekIndex++)];
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        }
        return result;
    }

    @Override
    public Event stepIntoContainer() throws IOException {
        if (isRefillable) {
            return stepInRefillable();
        }
        if (valueTid == null || !IonType.isContainer(valueTid.type)) {
            throw new IOException("Must be positioned on a container to step in.");
        }
        // Push the remaining length onto the stack, seek past the container's header, and increase the depth.
        ContainerInfo containerInfo = containerStack.push();
        containerInfo.type = valueTid.type;
        containerInfo.endIndex = valueMarker.endIndex;
        reset();
        valueTid = null;
        event = Event.NEEDS_INSTRUCTION;
        return event;
    }

    @Override
    public Event stepOutOfContainer() throws IOException {
        if (isRefillable) {
            return stepOutRefillable();
        }
        ContainerInfo containerInfo = containerStack.pop();
        if (containerInfo == null) {
            // Note: this is IllegalStateException for consistency with the other binary IonReader implementation.
            throw new IllegalStateException("Cannot step out at top level.");
        }
        // Seek past the remaining bytes at this depth and pop fro the stack.
        peekIndex = containerInfo.endIndex;
        reset();
        event = Event.NEEDS_INSTRUCTION;
        valueTid = null;
        return event;
    }

    @Override
    public Event nextValue() throws IOException {
        if (isRefillable) {
            return nextRefillable();
        }
        event = Event.NEEDS_DATA;
        valueTid = null;
        while (true) {
            if (peekIndex < valueMarker.endIndex) {
                peekIndex = valueMarker.endIndex;
            }
            reset();
            if (checkContainerEnd()) {
                break;
            }
            if (peekIndex >= limit) {
                checkpoint = peekIndex;
                break;
            }
            int b;
            ContainerInfo parent = containerStack.peek();
            if (parent == null) { // Depth 0
                b = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
                if (b == IVM_START_BYTE) {
                    parseIvm();
                    continue;
                }
            } else if (parent.type == IonType.STRUCT) {
                fieldSid = (int) readVarUInt(); // TODO type alignment
                b = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
            } else {
                b = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
            }
            if (parseTypeID(b, false)) {
                break;
            }
        }
        return event;
    }

    @Override
    public Event fillValue() throws IOException {
        if (isRefillable) {
            return fillValueRefillable();
        }
        event = Event.VALUE_READY;
        return event;
    }

    @Override
    public Event getCurrentEvent() {
        return event;
    }

    int ionMajorVersion() {
        return majorVersion;
    }

    int ionMinorVersion() {
        return minorVersion;
    }

    /**
     * Returns the marker for the sequence of annotation symbol IDs on the current value. The startIndex of the
     * returned marker is the index of the first byte of the first annotation symbol ID in the sequence. The endIndex
     * of the returned marker is the index of the type ID byte of the value to which the annotations are applied.
     * @return  the marker.
     */
    Marker getAnnotationSidsMarker() {
        return annotationSidsMarker;
    }

    Marker getValueMarker() {
        return valueMarker;
    }

    boolean isAwaitingMoreData() {
        if (isRefillable) {
            return isAwaitingMoreDataRefillable();
        }
        return peekIndex > checkpoint;
    }

    // Type: Non-fixed (refillable)

    private enum CheckpointLocation {
        BEFORE_UNANNOTATED_TYPE_ID,
        BEFORE_ANNOTATED_TYPE_ID,
        AFTER_SCALAR_HEADER,
        AFTER_CONTAINER_HEADER
    }

    protected enum State {
        FILL,
        SEEK,
        READY,
        TERMINATED
    }

    protected final long available() {
        return availableAt(offset);
    }

    protected final long availableAt(long index) {
        return limit - index;
    }

    private void handleOversizedValue() throws IOException {
        // The value was oversized.
        oversizedValueHandler.onOversizedValue();
        if (state != State.TERMINATED) { // TODO note: not required
            // TODO reuse setCheckpoint
            seek(valueMarker.endIndex - offset - individualBytesSkippedWithoutBuffering);
            reportConsumedData(valueMarker.endIndex - checkpoint);
            checkpointLocation = CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID;
            peekIndex = offset;
            checkpoint = peekIndex;
            reset();
        }
        isSkippingCurrentValue = false;
        individualBytesSkippedWithoutBuffering = 0;
    }

    public Event nextRefillable() throws IOException {
        while (true) {
            Event event = current.nextValue();
            if (isSkippingCurrentValue) {
                handleOversizedValue();
                // The user has requested a value. Continue to the next one.
                continue;
                // This was a request to fill a value, but the request could not be completed because the value
                // was oversized. Convey that NEXT is now required.
            }
            return event;
        }
    }

    public Event fillValueRefillable() throws IOException {
        Event event = current.fillValue();
        if (isSkippingCurrentValue) {
            handleOversizedValue();
        }
        return event;
    }

    Event stepInRefillable() throws IOException {
        return current.stepIntoContainer(); // TODO performance experiment: compare to simple branching rather than delegation
    }

    Event stepOutRefillable() throws IOException {
        return current.stepOutOfContainer();
    }

    /**
     * Ensures that there is space for at least 'minimumNumberOfBytesRequired' additional bytes in the buffer,
     * growing the buffer if necessary. May consolidate buffered bytes, performing an in-order copy and resetting
     * indices such that the `readIndex` points to the same byte and the `writeIndex` is positioned after the last
     * byte that is available to read.
     * @param minimumNumberOfBytesRequired the minimum number of additional bytes to buffer.
     * @return true if the buffer has sufficient capacity; otherwise, false.
     */
    boolean ensureCapacity(long minimumNumberOfBytesRequired) {
        if (freeSpaceAt(offset) >= minimumNumberOfBytesRequired) {
            // No need to shift any bytes or grow the buffer.
            return true;
        }
        if (minimumNumberOfBytesRequired > maximumBufferSize) {
            isSkippingCurrentValue = true;
            return false;
        }
        long shortfall = minimumNumberOfBytesRequired - capacity;
        if (shortfall > 0) {
            // TODO consider doubling the size rather than growing in increments of the initial buffer size.
            // TODO ensure alignment to a power of 2?
            int newSize = (int) Math.min(capacity + Math.max(initialBufferSize, shortfall), maximumBufferSize);
            byte[] newBuffer = new byte[newSize];
            moveBytesToStartOfBuffer(newBuffer);
            capacity = newSize;
            buffer = newBuffer;
            byteBuffer = ByteBuffer.wrap(buffer, (int) offset, (int) capacity);
        } else {
            // The current capacity can accommodate the requested size; move the existing bytes to the beginning
            // to make room for the remaining requested bytes to be filled at the end.
            moveBytesToStartOfBuffer(buffer);
        }
        return true;
    }

    protected boolean carefulFillAt(long index, long numberOfBytes) throws IOException {
        long shortfall = numberOfBytes - availableAt(index);
        if (shortfall > 0) {
            bytesRequested = numberOfBytes + (index - offset);
            if (ensureCapacity(bytesRequested)) {
                // Fill all the free space, not just the shortfall; this reduces I/O.
                refill(freeSpaceAt(limit));
                shortfall = bytesRequested - available();
            } else {
                // The request cannot be satisfied, but not because data was unavailable. Return normally; it is the
                // caller's responsibility to recover.
                shortfall = 0;
            }
        }
        if (shortfall <= 0) {
            bytesRequested = 0;
            state = State.READY;
            return true;
        }
        //remainingBytesRequested = shortfall;
        state = State.FILL;
        return false;
    }

    /**
     * Moves all buffered (but not yet read) bytes from 'buffer' to the destination buffer. In total, `size`
     * bytes will be moved.
     * @param destinationBuffer the destination buffer, which may be 'buffer' itself or a new buffer.
     */
    private void moveBytesToStartOfBuffer(byte[] destinationBuffer) {
        long size = available();
        if (size > 0) {
            System.arraycopy(buffer, (int) offset, destinationBuffer, 0, (int) size);
        }
        if (offset > 0) {
            // The existing data in the buffer has been shifted to the start. Adjust the saved indexes
            // accordingly. -1 indicates that all indices starting at 0 will be shifted.
            shiftIndicesLeft(-1, (int) offset);
        }
        offset = 0;
        //boundary = available;
        limit = size;
    }

    /**
     * @return the number of bytes that can be written at the end of the buffer.
     */
    private long freeSpaceAt(long index) {
        return capacity - index;
    }

    protected int peekByte() throws IOException {
        int b;
        if (isSkippingCurrentValue) {
            b = readByteWithoutBuffering();
            if (b >= 0) {
                individualBytesSkippedWithoutBuffering += 1;
            }
        } else {
            b = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
        }
        return b;
    }

    protected int carefulReadByte() throws IOException {
        int b;
        if (isSkippingCurrentValue) {
            // If the value is being skipped, the byte will not have been buffered.
            //b = getInput().read();
            b = readByteWithoutBuffering();
            if (b >= 0) {
                individualBytesSkippedWithoutBuffering += 1;
            }
        } else {
            if (!fillAt(peekIndex, 1)) {
                return -1;
            }
            // TODO ugly
            b = peekByte();
        }
        return b;
    }

    protected void enterQuickMode() {
        //current = quick; // TODO figure out how to activate
    }

    protected void exitQuickMode() {
        //current = careful;
    }

    // TODO inside Careful?
    final boolean fill(long numberOfBytes) throws IOException {
        return fillAt(offset, numberOfBytes);
    }

    final boolean fillAt(long index, long numberOfBytes) throws IOException {
        return carefulFillAt(index, numberOfBytes);
    }

    private final class Quick implements IonCursor {

        @Override
        public Event nextValue() throws IOException {
            return IonBinaryLexerBase.this.nextValue();
        }

        @Override
        public Event stepIntoContainer() throws IOException {
            return IonBinaryLexerBase.this.stepIntoContainer();
        }

        @Override
        public Event stepOutOfContainer() throws IOException {
            return IonBinaryLexerBase.this.stepOutOfContainer();
        }

        @Override
        public Event fillValue() throws IOException {
            return IonBinaryLexerBase.this.fillValue();
        }

        @Override
        public Event getCurrentEvent() {
            throw new IllegalStateException();
        }

        @Override
        public void close() throws IOException {
            throw new IllegalStateException();
        }
    }

    private final class Careful implements IonCursor {

        private int fillDepth = 0;

        private Event handleFill() {
            if (isSkippingCurrentValue) {
                return Event.NEEDS_INSTRUCTION;
            }
            if (checkpointLocation == CheckpointLocation.AFTER_CONTAINER_HEADER) {
                // This container is buffered in its entirety. There is no need to fill the buffer again until stepping
                // out of the fill depth.
                fillDepth = containerStack.size() + 1;
                // TODO could go into quick mode now, but it would need to be reset if this container is skipped
            }
            return Event.VALUE_READY;
        }

        private boolean makeBufferReady() throws IOException {
            boolean isReady;
            switch (state) {
                case READY:
                    isReady = true;
                    break;
                case SEEK:
                    isReady = seek(bytesRequested);
                    break;
                case FILL:
                    isReady = fill(bytesRequested);
                    break;
                case TERMINATED:
                    isReady = false;
                    break;
                default:
                    throw new IllegalStateException();
            }
            if (!isReady) {
                event = Event.NEEDS_DATA; // TODO should this be set in seek/fill to avoid the extra check?
            }
            return isReady;
        }

        private void setCheckpoint(CheckpointLocation location) {
            if (location == CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID) {
                reset();
                quickSeekTo(peekIndex);
            }
            reportConsumedData(peekIndex - checkpoint);
            checkpointLocation = location;
            checkpoint = peekIndex;
        }

        private boolean skipRemainingValueBytes() throws IOException {
            if (limit >= valueMarker.endIndex) {
                quickSeekTo(valueMarker.endIndex);
            } else if (!seekTo(valueMarker.endIndex)) {
                return true;
            }
            peekIndex = offset;

            if (peekIndex < valueMarker.endIndex) {
                shiftContainerEnds(valueMarker.endIndex - peekIndex);
            }
            setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
            return false;
        }

        public boolean parseAnnotationWrapperHeader(IonTypeID valueTid) throws IOException {
            long valueLength;
            int minimumAdditionalBytesNeeded;
            if (valueTid.variableLength) {
                // At this point the value must be at least 4 more bytes: 1 for the smallest-possible wrapper length, 1
                // for the smallest-possible annotations length, one for the smallest-possible annotation, and 1 for the
                // smallest-possible value representation.
                minimumAdditionalBytesNeeded = 4;
                if (!fillAt(peekIndex, minimumAdditionalBytesNeeded)) {
                    return true;
                }
                valueLength = readVarUInt(minimumAdditionalBytesNeeded);
                if (valueLength < 0) {
                    return true;
                }
            } else {
                // At this point the value must be at least 3 more bytes: 1 for the smallest-possible annotations
                // length, 1 for the smallest-possible annotation, and 1 for the smallest-possible value representation.
                minimumAdditionalBytesNeeded = 3;
                if (!fillAt(peekIndex, minimumAdditionalBytesNeeded)) {
                    return true;
                }
                valueLength = valueTid.length;
            }
            // Record the post-length index in a value that will be shifted in the even the buffer needs to refill.

            setValueMarker(valueLength, false);
            int annotationsLength = (int) readVarUInt(minimumAdditionalBytesNeeded);
            if (annotationsLength < 0) {
                return true;
            }
            if (!fillAt(peekIndex, annotationsLength)) {
                return true;
            }
            annotationSidsMarker.startIndex = peekIndex;
            annotationSidsMarker.endIndex = annotationSidsMarker.startIndex + annotationsLength;
            peekIndex = annotationSidsMarker.endIndex;
            if (peekIndex >= valueMarker.endIndex) {
                throw new IonException("Annotation wrapper must wrap a value.");
            }
            setCheckpoint(CheckpointLocation.BEFORE_ANNOTATED_TYPE_ID);
            return false;
        }

        public boolean parseValueHeader(IonTypeID valueTid, boolean isAnnotated) throws IOException {
            long valueLength;
            if (valueTid.variableLength) {
                // At this point the value must be at least 2 more bytes: 1 for the smallest-possible value length
                // and 1 for the smallest-possible value representation.
                if (!fillAt(peekIndex, 2)) {
                    return true;
                }
                valueLength = readVarUInt(2);
                if (valueLength < 0) {
                    return true;
                }
            } else {
                valueLength = valueTid.length;
            }
            if (IonType.isContainer(valueTid.type)) {
                setCheckpoint(CheckpointLocation.AFTER_CONTAINER_HEADER);
                event = Event.START_CONTAINER;
            } else if (valueTid.isNopPad) {
                if (isAnnotated) {
                    throw new IonException(
                        "Invalid annotation wrapper: NOP pad may not occur inside an annotation wrapper."
                    );
                }
                if (!seekTo(peekIndex + valueLength)) {
                    event = Event.NEEDS_DATA;
                    return true;
                }
                peekIndex = offset;
                valueLength = 0;
                setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
                checkContainerEnd();
            } else {
                setCheckpoint(CheckpointLocation.AFTER_SCALAR_HEADER);
                event = Event.START_SCALAR;
            }
            setValueMarker(valueLength, isAnnotated);
            return false;
        }

        /**
         * Reads the type ID byte.
         *
         * @param isAnnotated true if this type ID is on a value within an annotation wrapper; false if it is not.
         * @throws IOException if thrown by the underlying InputStream.
         */
        public boolean parseTypeID(final int typeIdByte, final boolean isAnnotated) throws IOException {
            IonTypeID valueTid = IonTypeID.TYPE_IDS[typeIdByte];
            if (!valueTid.isValid) {
                throw new IonException("Invalid type ID.");
            } else if (valueTid.type == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
                // Annotation.
                if (isAnnotated) {
                    throw new IonException("Nested annotation wrappers are invalid.");
                }
                if (parseAnnotationWrapperHeader(valueTid)) {
                    return true;
                }
            } else {
                if (parseValueHeader(valueTid, isAnnotated)) {
                    return true;
                }
            }
            IonBinaryLexerBase.this.valueTid = valueTid;
            if (checkpointLocation == CheckpointLocation.AFTER_SCALAR_HEADER) {
                return true;
            }
            if (checkpointLocation == CheckpointLocation.AFTER_CONTAINER_HEADER) {
                prohibitEmptyOrderedStruct();
                return true;
            }
            return false;
        }

        void quickSeekTo(long index) {
            offset = index;
        }

        boolean seekTo(long index) throws IOException {
            return seek(index - offset);
        }

        /**
         * Reads a VarUInt. NOTE: the VarUInt must fit in a `long`. This is not a true limitation, as IonJava requires
         * VarUInts to fit in an `int`.
         *
         * @param knownAvailable the number of bytes starting at 'peekIndex' known to be available in the buffer.
         */
        public long readVarUInt(int knownAvailable) throws IOException {
            int currentByte;
            int numberOfBytesRead = 0;
            long value = 0;
            while (numberOfBytesRead < knownAvailable) {
                currentByte = peekByte();
                numberOfBytesRead++;
                value = (value << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
                if ((currentByte & HIGHEST_BIT_BITMASK) != 0) {
                    return value;
                }
            }
            while (numberOfBytesRead < MAXIMUM_SUPPORTED_VAR_UINT_BYTES) {
                currentByte = carefulReadByte();
                if (currentByte < 0) {
                    return -1;
                }
                numberOfBytesRead++;
                value = (value << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
                if ((currentByte & HIGHEST_BIT_BITMASK) != 0) {
                    return value;
                }
            }
            throw new IonException("Found a VarUInt that was too large to fit in a `long`");
        }

        public boolean readFieldSid() throws IOException {
            // The value must have at least 2 more bytes: 1 for the smallest-possible field SID and 1 for
            // the smallest-possible representation.
            if (!fillAt(peekIndex, 2)) {
                return true;
            }
            fieldSid = (int) readVarUInt(2); // TODO type alignment
            return fieldSid < 0;
        }

        private void nextValueHelper() throws IOException {
            peekIndex = checkpoint;
            event = Event.NEEDS_DATA;
            valueTid = null;
            while (true) {
                if (!makeBufferReady() || checkContainerEnd()) {
                    return;
                }
                switch (checkpointLocation) {
                    case BEFORE_UNANNOTATED_TYPE_ID:
                        if (!containerStack.isEmpty() && containerStack.peek().type == IonType.STRUCT && readFieldSid()) {
                            return;
                        }
                        int b = carefulReadByte();
                        if (b < 0) {
                            return;
                        }
                        if (b == IVM_START_BYTE && containerStack.isEmpty()) {
                            if (!fillAt(peekIndex, IVM_REMAINING_LENGTH)) {
                                return;
                            }
                            parseIvm();
                            setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
                            continue;
                        }
                        if (parseTypeID(b, false)) {
                            return;
                        }
                        // Either a NOP has been skipped, or an annotation wrapper has been consumed.
                        continue;
                    case BEFORE_ANNOTATED_TYPE_ID:
                        b = carefulReadByte();
                        if (b < 0) {
                            return;
                        }
                        parseTypeID(b, true);
                        // If already within an annotation wrapper, neither an IVM nor a NOP is possible, so the lexer
                        // must be positioned after the header for the wrapped value.
                        return;
                    case AFTER_SCALAR_HEADER:
                    case AFTER_CONTAINER_HEADER: // TODO can we unify these two states?
                        if (skipRemainingValueBytes()) {
                            return;
                        }
                        // The previous value's bytes have now been skipped; continue.
                }
            }
        }

        @Override
        public Event nextValue() throws IOException {
            nextValueHelper();
            return event;
        }


        /**
         * @return a marker for the buffered value, or null if the value is not yet completely buffered.
         * @throws Exception
         */
        @Override
        public Event fillValue() throws IOException {
            if (!makeBufferReady()) {
                return event;
            }
            // Must be positioned on a scalar.
            if (checkpointLocation != CheckpointLocation.AFTER_SCALAR_HEADER && checkpointLocation != CheckpointLocation.AFTER_CONTAINER_HEADER) {
                throw new IllegalStateException();
            }
            event = Event.NEEDS_DATA;

            if (limit >= valueMarker.endIndex || fillAt(peekIndex, valueMarker.endIndex - valueMarker.startIndex)) {
                event = handleFill();
            }
            return event;
        }

        @Override
        public Event getCurrentEvent() {
            throw new IllegalStateException();
        }

        @Override
        public Event stepIntoContainer() throws IOException {
            if (!makeBufferReady()) {
                return event;
            }
            // Must be positioned on a container.
            if (checkpointLocation != CheckpointLocation.AFTER_CONTAINER_HEADER) {
                throw new IonException("Must be positioned on a container to step in.");
            }
            // Push the remaining length onto the stack, seek past the container's header, and increase the depth.
            ContainerInfo containerInfo = containerStack.push();
            if (containerStack.size() == fillDepth) {
                enterQuickMode();
            }
            containerInfo.type = valueTid.type;
            containerInfo.endIndex = valueMarker.endIndex;
            setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
            valueTid = null;
            event = Event.NEEDS_INSTRUCTION;
            return event;
        }

        @Override
        public Event stepOutOfContainer() throws IOException {
            ContainerInfo containerInfo = containerStack.peek();
            if (containerInfo == null) {
                // Note: this is IllegalStateException for consistency with the other binary IonReader implementation.
                throw new IllegalStateException("Cannot step out at top level.");
            }
            if (!makeBufferReady()) {
                return event;
            }
            // Seek past the remaining bytes at this depth, pop from the stack, and subtract the number of bytes
            // consumed at the previous depth from the remaining bytes needed at the current depth.
            event = Event.NEEDS_DATA;
            // Seek past any remaining bytes from the previous value.
            if (!seekTo(containerInfo.endIndex)) {
                return event;
            }
            peekIndex = containerInfo.endIndex;
            setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
            containerStack.pop();
            if (containerStack.size() < fillDepth) {
                fillDepth = 0;
                exitQuickMode();
            }
            event = Event.NEEDS_INSTRUCTION;
            valueTid = null;
            return event;
        }

        @Override
        public void close() throws IOException {
            throw new IllegalStateException();
        }
    }

    private long amountToShift = 0;

    private final _Private_RecyclingStack.Consumer<ContainerInfo> shiftContainerIndex =
        new _Private_RecyclingStack.Consumer<ContainerInfo>() {

            @Override
            public void accept(ContainerInfo element) {
                element.endIndex -= amountToShift;
            }
        };

    private void shiftContainerEnds(long shiftAmount) {
        amountToShift = shiftAmount; // TODO ugly
        containerStack.forEach(shiftContainerIndex);
        amountToShift = 0;
    }

    /**
     * Shift all indices after 'afterIndex' left by the given amount. This is used when data is moved in the underlying
     * buffer either due to buffer growth or NOP padding being reclaimed to make room for a value that would otherwise
     * exceed the buffer's maximum size.
     * @param afterIndex all indices after this index will be shifted (-1 indicates that all indices should be shifted).
     * @param shiftAmount the amount to shift left.
     */
    private void shiftIndicesLeft(int afterIndex, int shiftAmount) {
        peekIndex = Math.max(peekIndex - shiftAmount, 0);
        valueMarker.startIndex -= shiftAmount;
        valueMarker.endIndex -= shiftAmount;
        checkpoint -= shiftAmount;
        if (annotationSidsMarker.startIndex > afterIndex) {
            annotationSidsMarker.startIndex -= shiftAmount;
            annotationSidsMarker.endIndex -= shiftAmount;
        }
        shiftContainerEnds(shiftAmount);
    }

    void terminate() {
        state = State.TERMINATED;
    }

    boolean isAwaitingMoreDataRefillable() {
        return state != State.TERMINATED
            && (checkpointLocation.ordinal() > CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID.ordinal()
            || state == State.SEEK
            || bytesRequested > 1
            || peekIndex > checkpoint);
    }

    // Source: InputStream

    void refill(long numberOfBytesToFill) throws IOException {
        int numberOfBytesFilled = inputStream.read(buffer, (int) limit, (int) numberOfBytesToFill);
        if (numberOfBytesFilled < 0) {
            return;
        }
        limit += numberOfBytesFilled;
    }

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

    int readByteWithoutBuffering() throws IOException {
        return inputStream.read();
    }

    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}
