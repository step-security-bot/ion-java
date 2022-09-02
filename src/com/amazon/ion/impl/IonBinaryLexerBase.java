package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonType;

import java.io.IOException;
import java.io.InputStream;

abstract class IonBinaryLexerBase<Buffer extends AbstractBuffer> implements IonCursor {

    private static final int LOWER_SEVEN_BITS_BITMASK = 0x7F;
    private static final int HIGHEST_BIT_BITMASK = 0x80;
    private static final int VALUE_BITS_PER_VARUINT_BYTE = 7;
    // Note: because long is a signed type, Long.MAX_VALUE is represented in Long.SIZE - 1 bits.
    private static final int MAXIMUM_SUPPORTED_VAR_UINT_BYTES = (Long.SIZE - 1) / VALUE_BITS_PER_VARUINT_BYTE;
    private static final int IVM_START_BYTE = 0xE0;
    private static final int IVM_FINAL_BYTE = 0xEA;
    private static final int IVM_REMAINING_LENGTH = 3; // Length of the IVM after the first byte.

    private static final BufferConfiguration.DataHandler NO_OP_DATA_HANDLER = new BufferConfiguration.DataHandler() {
        @Override
        public void onData(long numberOfBytes) {
            // Do nothing.
        }
    };

    protected enum CheckpointLocation {
        BEFORE_UNANNOTATED_TYPE_ID,
        BEFORE_ANNOTATED_TYPE_ID,
        AFTER_SCALAR_HEADER,
        AFTER_CONTAINER_HEADER
    }

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
        private IonType type;

        /**
         * The byte position of the end of the container.
         */
        long endIndex;

        void set(IonType type, long endIndex) {
            this.type = type;
            this.endIndex = endIndex;
        }
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

    protected final Buffer buffer;

    /**
     * The handler that will be notified when data is processed.
     */
    protected final BufferConfiguration.DataHandler dataHandler;

    /**
     * Marker for the sequence of annotation symbol IDs on the current value. If there are no annotations on the
     * current value, the startIndex will be negative.
     */
    protected final Marker annotationSidsMarker = new IonBinaryLexerRefillable.Marker(-1, 0);

    protected final Marker valueMarker = new IonBinaryLexerRefillable.Marker(-1, 0);

    private final IvmNotificationConsumer ivmConsumer;

    private IonCursor.Event event = IonCursor.Event.NEEDS_DATA;

    // The major version of the Ion encoding currently being read.
    private int majorVersion = -1;

    // The minor version of the Ion encoding currently being read.
    private int minorVersion = 0;

    /**
     * The type ID byte of the current value.
     */
    private IonTypeID valueTid;

    protected CheckpointLocation checkpointLocation = CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID;

    private int fieldSid;
    
    protected long checkpoint;

    protected long peekIndex;

    private final LexerVariant careful = new Careful();

    private final LexerVariant quick = new Quick();

    private LexerVariant current;

    IonBinaryLexerBase(
        final Buffer buffer,
        final BufferConfiguration.DataHandler dataHandler,
        final IvmNotificationConsumer ivmConsumer
    ) {
        this.buffer = buffer;
        this.dataHandler = dataHandler == null ? NO_OP_DATA_HANDLER : dataHandler;
        this.ivmConsumer = ivmConsumer;
        containerStack = new _Private_RecyclingStack<ContainerInfo>(
            CONTAINER_STACK_INITIAL_CAPACITY,
            CONTAINER_INFO_FACTORY
        );
        peekIndex = buffer.getOffset();
        checkpoint = peekIndex;
        current = (buffer instanceof RefillableBuffer) ? careful : quick;
    }

    protected abstract int readByte() throws IOException;

    protected void setValueMarker(long valueLength, boolean isAnnotated) {
        long endIndex = peekIndex + valueLength;
        if (!containerStack.isEmpty()) {
            if (endIndex > containerStack.peek().endIndex) {
                throw new IonException("Value exceeds the length of its parent container.");
            }
        }
        if (isAnnotated && endIndex != valueMarker.endIndex) {
            // valueMarker.endIndex refers to the end of the annotation wrapper.
            throw new IonException("Annotation wrapper length does not match the length of the wrapped value.");
        }
        valueMarker.startIndex = peekIndex;
        valueMarker.endIndex = endIndex;
    }

    private boolean checkContainerEnd() {
        ContainerInfo parent = containerStack.peek();
        if (parent == null || parent.endIndex > peekIndex) {
            return false;
        }
        if (parent.endIndex == peekIndex) {
            event = Event.END_CONTAINER;
            valueTid = null;
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

    protected void reportConsumedData(long numberOfBytesToReport) {
        dataHandler.onData(numberOfBytesToReport);
    }

    private void parseIvm() {
        majorVersion = buffer.peek(peekIndex++);
        minorVersion = buffer.peek(peekIndex++);
        if (buffer.peek(peekIndex++) != IVM_FINAL_BYTE) {
            throw new IonException("Invalid Ion version marker.");
        }
        if (majorVersion != 1 || minorVersion != 0) {
            throw new IonException(String.format("Unsupported Ion version: %d.%d", majorVersion, minorVersion));
        }
        ivmConsumer.ivmEncountered(majorVersion, minorVersion);
    }

    protected int peekByte() throws IOException {
        return buffer.peek(peekIndex++);
    }

    private void prohibitEmptyOrderedStruct() {
        if (valueTid.type == IonType.STRUCT &&
            valueTid.lowerNibble == IonTypeID.ORDERED_STRUCT_NIBBLE &&
            valueMarker.endIndex == peekIndex
        ) {
            throw new IonException("Ordered struct must not be empty.");
        }
    }

    protected void handleSkip() {
        // Nothing to do.
    }

    private int fillDepth = 0;

    protected Event handleFill() {
        if (checkpointLocation == CheckpointLocation.AFTER_CONTAINER_HEADER) {
            // This container is buffered in its entirety. There is no need to fill the buffer again until stepping
            // out of the fill depth.
            fillDepth = getDepth() + 1;
            // TODO could go into quick mode now, but it would need to be reset if this container is skipped
        }
        return Event.VALUE_READY;
    }

    protected void enterQuickMode() {
        buffer.quick();
        currentMakeBufferReadyFunction = QUICK_MAKE_BUFFER_READY_FUNCTION;
        //current = quick;
    }

    protected void exitQuickMode() {
        buffer.careful();
        currentMakeBufferReadyFunction = carefulMakeBufferReadyFunction;
        //current = careful;
    }

    private interface LexerVariant {
        boolean parseAnnotationWrapperHeader(IonTypeID valueTid) throws IOException;
        boolean parseValueHeader(IonTypeID valueTid, boolean isAnnotated) throws IOException;
        boolean parseTypeID(final int typeIdByte, final boolean isAnnotated) throws IOException;
        long readVarUInt(int knownAvailable) throws IOException;
        boolean readFieldSid() throws IOException;
        void nextHeader() throws IOException;
        void fillValue() throws IOException;
        void stepIn() throws IOException;
        void stepOut() throws IOException;
    }

    private class Careful implements LexerVariant {

        void setCheckpoint(CheckpointLocation location) {
            if (location == CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID) {
                reset();
                buffer.quickSeekTo(peekIndex);
            }
            reportConsumedData(peekIndex - checkpoint);
            checkpointLocation = location;
            checkpoint = peekIndex;
        }

        protected boolean skipRemainingValueBytes() throws IOException {
            if (buffer.limit >= valueMarker.endIndex) {
                buffer.quickSeekTo(valueMarker.endIndex);
            } else if (!buffer.seekTo(valueMarker.endIndex)) {
                return true;
            }
            peekIndex = buffer.getOffset();

            handleSkip();
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
                if (!buffer.fillAt(peekIndex, minimumAdditionalBytesNeeded)) {
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
                if (!buffer.fillAt(peekIndex, minimumAdditionalBytesNeeded)) {
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
            if (!buffer.fillAt(peekIndex, annotationsLength)) {
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
            if (valueTid.isNull || valueTid.type == IonType.BOOL) {
                // null values are always a single byte.
                valueLength = 0;
            } else {
                if (valueTid.variableLength) {
                    // At this point the value must be at least 2 more bytes: 1 for the smallest-possible value length
                    // and 1 for the smallest-possible value representation.
                    if (!buffer.fillAt(peekIndex, 2)) {
                        return true;
                    }
                    valueLength = readVarUInt(2);
                    if (valueLength < 0) {
                        return true;
                    }
                } else {
                    valueLength = valueTid.length;
                }
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
                if (!buffer.seekTo(peekIndex + valueLength)) {
                    event = Event.NEEDS_DATA;
                    return true;
                }
                peekIndex = buffer.getOffset();
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
            if (!buffer.fillAt(peekIndex, 2)) {
                return true;
            }
            fieldSid = (int) readVarUInt(2); // TODO type alignment
            return fieldSid < 0;
        }

        public void nextHeader() throws IOException {
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
                            if (!buffer.fillAt(peekIndex, IVM_REMAINING_LENGTH)) {
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


        /**
         * @return a marker for the buffered value, or null if the value is not yet completely buffered.
         * @throws Exception
         */
        public void fillValue() throws IOException {
            if (!makeBufferReady()) {
                return;
            }
            // Must be positioned on a scalar.
            if (checkpointLocation != CheckpointLocation.AFTER_SCALAR_HEADER && checkpointLocation != CheckpointLocation.AFTER_CONTAINER_HEADER) {
                throw new IllegalStateException();
            }
            event = Event.NEEDS_DATA;

            if (buffer.limit >= valueMarker.endIndex || buffer.fillAt(peekIndex, valueMarker.endIndex - valueMarker.startIndex)) {
                event = handleFill();
            }
        }

        public void stepIn() throws IOException {
            if (!makeBufferReady()) {
                return;
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
        }

        public void stepOut() throws IOException {
            ContainerInfo containerInfo = containerStack.peek();
            if (containerInfo == null) {
                // Note: this is IllegalStateException for consistency with the other binary IonReader implementation.
                throw new IllegalStateException("Cannot step out at top level.");
            }
            if (!makeBufferReady()) {
                return;
            }
            // Seek past the remaining bytes at this depth, pop from the stack, and subtract the number of bytes
            // consumed at the previous depth from the remaining bytes needed at the current depth.
            event = Event.NEEDS_DATA;
            // Seek past any remaining bytes from the previous value.
            if (!buffer.seekTo(containerInfo.endIndex)) {
                return;
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
        }
    }

    private class Quick implements LexerVariant {

        public boolean parseAnnotationWrapperHeader(IonTypeID valueTid) throws IOException {
            long valueLength;
            if (valueTid.variableLength) {
                valueLength = readVarUInt(0);
            } else {
                valueLength = valueTid.length;
            }
            setValueMarker(valueLength, false);
            if (valueMarker.endIndex > buffer.limit) {
                return true;
            }
            int annotationsLength = (int) readVarUInt(0);
            annotationSidsMarker.startIndex = peekIndex;
            annotationSidsMarker.endIndex = annotationSidsMarker.startIndex + annotationsLength;
            peekIndex = annotationSidsMarker.endIndex;
            if (peekIndex >= valueMarker.endIndex) {
                throw new IonException("Annotation wrapper must wrap a value.");
            }
            return false;
        }

        public boolean parseValueHeader(IonTypeID valueTid, boolean isAnnotated) throws IOException {
            long valueLength;
            if (valueTid.isNull || valueTid.type == IonType.BOOL) {
                // null values are always a single byte.
                valueLength = 0;
            } else {
                if (valueTid.variableLength) {
                    valueLength = readVarUInt(2);
                } else {
                    valueLength = valueTid.length;
                }
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
                if (destination > buffer.limit) {
                    throw new IonException("Invalid NOP pad.");
                }
                peekIndex += valueLength;
                valueLength = 0;
                checkContainerEnd();
            } else {
                event = Event.START_SCALAR;
            }
            setValueMarker(valueLength, isAnnotated);
            if (valueMarker.endIndex > buffer.limit) {
                event = Event.NEEDS_DATA;
                return true;
            }
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
         * @param knownAvailable the number of bytes starting at 'peekIndex' known to be available in the buffer.
         */
        public long readVarUInt(int knownAvailable) throws IOException {
            int currentByte;
            int numberOfBytesRead = 0;
            long value = 0;
            while (numberOfBytesRead < MAXIMUM_SUPPORTED_VAR_UINT_BYTES) {
                currentByte = peekByte();
                numberOfBytesRead++;
                value = (value << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
                if ((currentByte & HIGHEST_BIT_BITMASK) != 0) {
                    return value;
                }
            }
            throw new IonException("Found a VarUInt that was too large to fit in a `long`");
        }

        public boolean readFieldSid() throws IOException {
            fieldSid = (int) readVarUInt(0); // TODO type alignment
            return false;
        }

        public void nextHeader() throws IOException {
            event = Event.NEEDS_DATA;
            valueTid = null;
            while (true) {
                if (peekIndex < valueMarker.endIndex) {
                    peekIndex = valueMarker.endIndex;
                }
                reset();
                if (checkContainerEnd()) {
                    return;
                }
                if (peekIndex >= buffer.limit) {
                    checkpoint = peekIndex;
                    return;
                }
                int b;
                ContainerInfo parent = containerStack.peek();
                if (parent == null) { // Depth 0
                    b = peekByte();
                    if (b == IVM_START_BYTE) {
                        parseIvm();
                        continue;
                    }
                } else if (parent.type == IonType.STRUCT) {
                    readFieldSid();
                    b = peekByte();
                } else {
                    b = peekByte();
                }
                if (parseTypeID(b, false)) {
                    return;
                }
                if (valueTid.type == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
                    parseTypeID(peekByte(), true); // TODO see about making this recursive in parseAnnotationHeader
                    return;
                }
            }
        }

        /**
         * @return a marker for the buffered value, or null if the value is not yet completely buffered.
         * @throws Exception
         */
        public void fillValue() throws IOException {
            event = Event.VALUE_READY;
        }

        public void stepIn() throws IOException {
            if (!IonType.isContainer(getType())) {
                throw new IOException("Must be positioned on a container to step in.");
            }
            // Push the remaining length onto the stack, seek past the container's header, and increase the depth.
            ContainerInfo containerInfo = containerStack.push();
            containerInfo.set(valueTid.type, valueMarker.endIndex);
            reset();
            valueTid = null;
            event = Event.NEEDS_INSTRUCTION;
        }

        public void stepOut() throws IOException {
            ContainerInfo containerInfo = containerStack.peek();
            if (containerInfo == null) {
                // Note: this is IllegalStateException for consistency with the other binary IonReader implementation.
                throw new IllegalStateException("Cannot step out at top level.");
            }
            // Seek past the remaining bytes at this depth and pop fro the stack.
            peekIndex = containerInfo.endIndex;
            reset();
            containerStack.pop();
            event = Event.NEEDS_INSTRUCTION;
            valueTid = null;
        }
    }

    private interface MakeBufferReadyFunction {
        boolean makeBufferReady() throws IOException;
    }

    private final MakeBufferReadyFunction carefulMakeBufferReadyFunction = new MakeBufferReadyFunction() {
        @Override
        public boolean makeBufferReady() throws IOException {
            if (!buffer.makeReady()) {
                event = Event.NEEDS_DATA;
                return false;
            }
            return true;
        }
    };

    private static final MakeBufferReadyFunction QUICK_MAKE_BUFFER_READY_FUNCTION = new MakeBufferReadyFunction() {
        @Override
        public boolean makeBufferReady() {
            return true;
        }
    };

    private MakeBufferReadyFunction currentMakeBufferReadyFunction = carefulMakeBufferReadyFunction;

    private boolean makeBufferReady() throws IOException {
        return currentMakeBufferReadyFunction.makeBufferReady();
    }

    @Override
    public Event next(Instruction instruction) throws IOException {
        switch (instruction) {
            case STEP_IN:
                current.stepIn();
                break;
            case NEXT_VALUE:
                current.nextHeader();
                break;
            case LOAD_VALUE:
                current.fillValue();
                break;
            case STEP_OUT:
                current.stepOut();
                break;
        }
        return event;
    }

    @Override
    public Event getCurrentEvent() {
        return event;
    }

    @Override
    public void fill(InputStream inputStream) {
        // TODO
    }

    int ionMajorVersion() {
        return majorVersion;
    }

    int ionMinorVersion() {
        return minorVersion;
    }

    /**
     * @return the type ID of the current value.
     */
    IonTypeID getValueTid() {
        return valueTid;
    }

    /**
     * @return true if the current value has annotations; otherwise, false.
     */
    boolean hasAnnotations() {
        return annotationSidsMarker.startIndex >= 0;
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

    public boolean isInStruct() {
        return !containerStack.isEmpty() && containerStack.peek().type == IonType.STRUCT;
    }

    int getFieldId() {
        // TODO see if it's possible to simplify this
        return valueTid == null ? -1 : fieldSid;
    }

    public int getDepth() {
        return containerStack.size();
    }

    public IonType getType() {
        return valueTid == null ? null : valueTid.type;
    }

    IonType peekType() {
        IonType type = getType();
        // TODO verify this complexity is warranted
        if (type == null && buffer.isReady() && buffer.available() > 0) {
            IonTypeID valueTid = IonTypeID.TYPE_IDS[buffer.peek(checkpoint)];
            if (valueTid.type != IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
                type = valueTid.type;
            }
        }
        return type;
    }

    boolean isTopLevel() {
        return containerStack.isEmpty();
    }

    boolean isAwaitingMoreData() {
        return !buffer.isTerminated()
            && (peekIndex > checkpoint
                || checkpointLocation.ordinal() > CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID.ordinal()
                || buffer.isAwaitingMoreData());
    }

    @Override
    public void close() throws IOException {
        // Nothing to do.
    }
}
