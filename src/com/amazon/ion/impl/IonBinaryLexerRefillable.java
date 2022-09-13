package com.amazon.ion.impl;

import static com.amazon.ion.IonCursor.Event;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class IonBinaryLexerRefillable extends IonBinaryLexerBase {

    /**
     * Handler of notifications provided by the ResizingPipedInputStream.
     */
    interface NotificationConsumer {

        /**
         * Bytes have been shifted to the start of the buffer in order to make room for additional bytes
         * to be buffered.
         * @param leftShiftAmount the amount of the left shift (also: the pre-shift read index of the first shifted
         *                        byte).
         */
        void bytesConsolidatedToStartOfBuffer(int leftShiftAmount);

        void bufferOverflowDetected();
    }

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

    // TODO should this be an IonBufferConfiguration?
    private final BufferConfiguration<?> configuration;

    /**
     * The initial size of the buffer and the number of bytes by which the size of the buffer will increase
     * each time it grows, unless it must grow by a smaller amount to fit within 'maximumBufferSize'.
     */
    protected final int initialBufferSize;

    /**
     * The maximum size of the buffer. If the user attempts to buffer more bytes than this, an exception will be raised.
     */
    protected final int maximumBufferSize;


    protected NotificationConsumer notificationConsumer;

    protected byte[] buffer;

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

    IonBinaryLexerRefillable(final BufferConfiguration<?> configuration) {
        super(0, configuration.getDataHandler());
        if (configuration.getInitialBufferSize() < 1) {
            throw new IllegalArgumentException("Initial buffer size must be at least 1.");
        }
        if (configuration.getMaximumBufferSize() < configuration.getInitialBufferSize()) {
            throw new IllegalArgumentException("Maximum buffer size cannot be less than the initial buffer size.");
        }
        this.configuration = configuration;
        this.initialBufferSize = configuration.getInitialBufferSize();
        this.maximumBufferSize = configuration.getMaximumBufferSize();
        this.capacity = initialBufferSize;
        buffer = new byte[initialBufferSize];
        byteBuffer = ByteBuffer.wrap(buffer, 0, initialBufferSize);
        notificationConsumer = new NotificationConsumer() {
            @Override
            public void bytesConsolidatedToStartOfBuffer(int leftShiftAmount) {
                // The existing data in the buffer has been shifted to the start. Adjust the saved indexes
                // accordingly. -1 indicates that all indices starting at 0 will be shifted.
                shiftIndicesLeft(-1, leftShiftAmount);
            }

            @Override
            public void bufferOverflowDetected() {
                isSkippingCurrentValue = true;
            }
        };
        pageSize = configuration.getInitialBufferSize();
        current = careful;
    }

    abstract void refill(long numberOfBytesToFill) throws IOException;

    abstract int readByteWithoutBuffering() throws IOException;

    // TODO is it possible to just put this in the constructor?
    void registerOversizedValueHandler(BufferConfiguration.OversizedValueHandler oversizedValueHandler) {
        this.oversizedValueHandler = oversizedValueHandler;
    }

    private void handleOversizedValue() throws IOException {
        // The value was oversized.
        oversizedValueHandler.onOversizedValue();
        if (!isTerminated()) { // TODO note: not required
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

    @Override
    public Event next() throws IOException {
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

    @Override
    public Event fillValue() throws IOException {
        Event event = current.fillValue();
        if (isSkippingCurrentValue) {
            handleOversizedValue();
        }
        return event;
    }

    @Override
    Event stepIn() throws IOException {
        return current.stepIntoContainer(); // TODO performance experiment: compare to simple branching rather than delegation
    }

    @Override
    Event stepOut() throws IOException {
        return current.stepOutOfContainer();
    }

    @Override
    int peek(long index) {
        return buffer[(int) index] & SINGLE_BYTE_MASK;
    }

    @Override
    void copyBytes(long position, byte[] destination, int destinationOffset, int length) {
        System.arraycopy(buffer, (int) position, destination, destinationOffset, length);
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
            notificationConsumer.bufferOverflowDetected();
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

    @Override
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
            notificationConsumer.bytesConsolidatedToStartOfBuffer((int) offset);
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

    // TODO remove. Only used within Careful, so there should only be one implementation
    private interface ReadByteFunction {
        int readByte() throws IOException;
    }

    private final ReadByteFunction carefulReadByteFunction = new ReadByteFunction() {
        @Override
        public int readByte() throws IOException {
            return carefulReadByte();
        }
    };

    private final ReadByteFunction quickReadByteFunction = new ReadByteFunction() {
        @Override
        public int readByte() throws IOException {
            return peekByte();
        }
    };

    private ReadByteFunction currentReadByteFunction = carefulReadByteFunction;

    @Override
    protected int peekByte() throws IOException {
        int b;
        if (isSkippingCurrentValue) {
            b = readByteWithoutBuffering();
            if (b >= 0) {
                individualBytesSkippedWithoutBuffering += 1;
            }
        } else {
            b = peek(peekIndex);
            //pipe.extendBoundary(1);
            peekIndex++;
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

    @Override
    protected void setValueMarker(long valueLength, boolean isAnnotated) {
        if (isSkippingCurrentValue) {
            // If the value is being skipped, not all of its bytes will be buffered, so start/end indexes will not
            // align to the expected values. This is fine, because the value will not be accessed.
            return;
        }
        super.setValueMarker(valueLength, isAnnotated);
    }

    protected void enterQuickMode() {
        quick();
        currentReadByteFunction = quickReadByteFunction;
        //current = quick;
    }

    protected void exitQuickMode() {
        careful();
        currentReadByteFunction = carefulReadByteFunction;
        //current = careful;
    }

    private final class Quick implements IonCursor {

        @Override
        public Event nextValue() throws IOException {
            return IonBinaryLexerRefillable.super.next();
        }

        @Override
        public Event stepIntoContainer() throws IOException {
            return IonBinaryLexerRefillable.super.stepIn();
        }

        @Override
        public Event stepOutOfContainer() throws IOException {
            return IonBinaryLexerRefillable.super.stepOut();
        }

        @Override
        public Event fillValue() throws IOException {
            return IonBinaryLexerRefillable.super.fillValue();
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
                fillDepth = getDepth() + 1;
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
            IonBinaryLexerRefillable.this.valueTid = valueTid;
            if (checkpointLocation == CheckpointLocation.AFTER_SCALAR_HEADER) {
                return true;
            }
            if (checkpointLocation == CheckpointLocation.AFTER_CONTAINER_HEADER) {
                prohibitEmptyOrderedStruct();
                return true;
            }
            return false;
        }


        /**
         * Reads one byte, if possible.
         * @return the byte, or -1 if none was available.
         * @throws IOException if an IOException is thrown by the underlying InputStream.
         */
        private int readByte() throws IOException {
            /*
            if (peekIndex >= limit) { // TODO any way to avoid?
                return -1;
            }
            return peek(peekIndex++);

             */
            return currentReadByteFunction.readByte();
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
                currentByte = readByte();
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
                        fieldSid = -1;
                        if (isInStruct() && readFieldSid()) {
                            return;
                        }
                        int b = readByte();
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
                        b = readByte();
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
            if (getDepth() == fillDepth) {
                enterQuickMode();
            }
            containerInfo.set(valueTid.type, valueMarker.endIndex);
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
            if (getDepth() < fillDepth) {
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

    @Override
    protected boolean isReady() {
        return state == State.READY;
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

    BufferConfiguration<?> getConfiguration() {
        return configuration;
    }

    void terminate() {
        state = State.TERMINATED;
    }

    boolean isTerminated() {
        return state == State.TERMINATED;
    }

    @Override
    boolean isAwaitingMoreData() {
        return !isTerminated()
            && (checkpointLocation.ordinal() > CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID.ordinal()
            || state == State.SEEK
            || super.isAwaitingMoreData());
    }
}
