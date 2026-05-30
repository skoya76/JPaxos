package lsr.paxos.test.etcdstyle;

import java.nio.ByteBuffer;

final class EtcdStyleProtocolCodec {
    static final byte REQUEST_NEW_ID = (byte) 'T';
    static final int COMMAND_REQUEST = 0;
    static final int REPLY_OK = 0;
    static final int REPLY_NACK = 1;
    static final int REPLY_REDIRECT = 2;
    static final int REPLY_RECONNECT = 3;

    private static final int CLIENT_COMMAND_HEADER_SIZE = 4 + 8 + 4 + 4;
    private static final int CLIENT_REPLY_HEADER_SIZE = 4 + 4;
    private static final int REPLY_VALUE_HEADER_SIZE = 8 + 4;

    private EtcdStyleProtocolCodec() {
    }

    static ByteBuffer encodeRequest(long clientId, int sequenceId, byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(CLIENT_COMMAND_HEADER_SIZE + value.length);
        buffer.putInt(COMMAND_REQUEST);
        buffer.putLong(clientId);
        buffer.putInt(sequenceId);
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.flip();
        return buffer;
    }

    static ClientReplyFrame tryDecodeClientReply(ByteBuffer buffer) {
        if (buffer.remaining() < CLIENT_REPLY_HEADER_SIZE) {
            return null;
        }

        buffer.mark();
        int result = buffer.getInt();
        int valueLength = buffer.getInt();
        if (valueLength < 0) {
            throw new IllegalStateException("Negative reply length: " + valueLength);
        }
        if (buffer.remaining() < valueLength) {
            buffer.reset();
            return null;
        }

        byte[] value = new byte[valueLength];
        buffer.get(value);

        long clientId = -1;
        int sequenceId = -1;
        if (result == REPLY_OK) {
            if (valueLength < REPLY_VALUE_HEADER_SIZE) {
                throw new IllegalStateException("Reply too short: " + valueLength);
            }
            ByteBuffer nested = ByteBuffer.wrap(value);
            clientId = nested.getLong();
            sequenceId = nested.getInt();
            byte[] payload = new byte[nested.remaining()];
            nested.get(payload);
            value = payload;
        }

        return new ClientReplyFrame(result, clientId, sequenceId, value);
    }

    static ByteBuffer encodeOkClientReply(long clientId, int sequenceId, byte[] value) {
        ByteBuffer nested = ByteBuffer.allocate(REPLY_VALUE_HEADER_SIZE + value.length);
        nested.putLong(clientId);
        nested.putInt(sequenceId);
        nested.put(value);
        nested.flip();

        ByteBuffer reply = ByteBuffer.allocate(CLIENT_REPLY_HEADER_SIZE + nested.remaining());
        reply.putInt(REPLY_OK);
        reply.putInt(nested.remaining());
        reply.put(nested);
        reply.flip();
        return reply;
    }

    static final class ClientReplyFrame {
        final int result;
        final long clientId;
        final int sequenceId;
        final byte[] value;

        ClientReplyFrame(int result, long clientId, int sequenceId, byte[] value) {
            this.result = result;
            this.clientId = clientId;
            this.sequenceId = sequenceId;
            this.value = value;
        }
    }
}
