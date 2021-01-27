package io.cowculate.streams;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;

public class StreamWriter {

    private final KinesisProducer producer;

    public StreamWriter(KinesisProducer producer) {
        this.producer = producer;
    }

    public void putRecord( byte[] bytes, String partitionKey) {

        ListenableFuture<UserRecordResult> future = producer.addUserRecord(
                Settings.KINESIS_DATA_STREAM,
                partitionKey,
                ByteBuffer.wrap(bytes)
        );
    }
}
