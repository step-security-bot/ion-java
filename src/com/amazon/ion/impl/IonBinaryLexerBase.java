package com.amazon.ion.impl;

import static com.amazon.ion.IonCursor.Event;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonType;

import java.io.IOException;
import java.nio.ByteBuffer;

// TODO removed 'implements IonCursor' as a performance experiment. Try adding back.
class IonBinaryLexerBase {

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

    protected int fieldSid;
    
    protected long checkpoint;

    protected long peekIndex;

    IonBinaryLexerBase(
        final BufferConfiguration<?> configuration,
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

        this.buffer = buffer;
        this.offset = offset;
        this.limit = offset + length;
        this.capacity = limit;
        byteBuffer = ByteBuffer.wrap(buffer, offset, length);
    }

    void registerIvmNotificationConsumer(IvmNotificationConsumer ivmConsumer) {
        this.ivmConsumer = ivmConsumer;
    }

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

    protected boolean checkContainerEnd() {
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

    // TODO test that this is called when necessary in quick/fixed mode
    protected void reportConsumedData(long numberOfBytesToReport) {
        dataHandler.onData(numberOfBytesToReport);
    }

    protected void parseIvm() {
        majorVersion = peek(peekIndex++);
        minorVersion = peek(peekIndex++);
        if (peek(peekIndex++) != IVM_FINAL_BYTE) {
            throw new IonException("Invalid Ion version marker.");
        }
        if (majorVersion != 1 || minorVersion != 0) {
            throw new IonException(String.format("Unsupported Ion version: %d.%d", majorVersion, minorVersion));
        }
        ivmConsumer.ivmEncountered(majorVersion, minorVersion);
    }

    protected int peekByte() throws IOException {
        return peek(peekIndex++);
    }

    protected void prohibitEmptyOrderedStruct() {
        if (valueTid.type == IonType.STRUCT &&
            valueTid.lowerNibble == IonTypeID.ORDERED_STRUCT_NIBBLE &&
            valueMarker.endIndex == peekIndex
        ) {
            throw new IonException("Ordered struct must not be empty.");
        }
    }

    private boolean parseAnnotationWrapperHeader(IonTypeID valueTid) throws IOException {
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

    private boolean parseValueHeader(IonTypeID valueTid, boolean isAnnotated) throws IOException {
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
     * @throws IOException if thrown by the underlying InputStream.
     */
    private boolean parseTypeID(final int typeIdByte, final boolean isAnnotated) throws IOException {
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
            return parseTypeID(peekByte(), true);
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
    private long readVarUInt() throws IOException {
        int currentByte = 0;
        long result = 0;
        while ((currentByte & HIGHEST_BIT_BITMASK) == 0) {
            currentByte = peekByte();
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        }
        return result;
    }

    Event stepIn() throws IOException {
        if (!IonType.isContainer(getType())) {
            throw new IOException("Must be positioned on a container to step in.");
        }
        // Push the remaining length onto the stack, seek past the container's header, and increase the depth.
        ContainerInfo containerInfo = containerStack.push();
        containerInfo.set(valueTid.type, valueMarker.endIndex);
        reset();
        valueTid = null;
        event = Event.NEEDS_INSTRUCTION;
        return event;
    }

    Event stepOut() throws IOException {
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
        return event;
    }

    Event next() throws IOException {
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
                b = peekByte();
                if (b == IVM_START_BYTE) {
                    parseIvm();
                    continue;
                }
            } else if (parent.type == IonType.STRUCT) {
                fieldSid = (int) readVarUInt(); // TODO type alignment
                b = peekByte();
            } else {
                b = peekByte();
            }
            if (parseTypeID(b, false)) {
                break;
            }
        }
        return event;
    }

    Event fillValue() throws IOException {
        event = Event.VALUE_READY;
        return event;
    }

    //@Override
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

    int peek(long index) {
        return buffer[(int) index] & SINGLE_BYTE_MASK;
    }

    void copyBytes(long position, byte[] destination, int destinationOffset, int length) {
        System.arraycopy(buffer, (int) position, destination, destinationOffset, length);
    }

    protected boolean isReady() {
        return true;
    }

    IonType peekType() {
        IonType type = getType();
        // TODO verify this complexity is warranted
        if (type == null && isReady() && available() > 0) {
            IonTypeID valueTid = IonTypeID.TYPE_IDS[peek(checkpoint)];
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
        return peekIndex > checkpoint;
    }

    //@Override
    public void close() throws IOException {
        // Nothing to do.
    }
}
