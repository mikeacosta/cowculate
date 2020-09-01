package io.cowculate.streams;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.cowculate.streams.Settings.SENSOR_COUNT;

public class Cowculator {

    public static void main(String[] args) throws InterruptedException {

        final KinesisProducer producer = createProducer();
        final StreamWriter writer = new StreamWriter(producer);

        processDataStream(writer);
    }

    private static void processDataStream(StreamWriter writer) throws InterruptedException {

        List<String> sensors = getListFromCsv("src/main/resources/cows.csv");

        while (true) {

            for (int i = 0; i < SENSOR_COUNT; i++) {
                long timeStamp = Instant.now().toEpochMilli();
                float bodyTemp = getRandomFloat(100.5f, 102.5f);
                int motion = getRandomNumber(1, 10);
                float rumination = getRandomFloat(0f, 1f);
                int sensorNo = i + 1;
                UUID eventId = UUID.randomUUID();

                String sensorEvent = String.format(
                        "{\"timestamp\":%s,\"body_temperature\":\"%s\",\"motion\":\"%s\",\"rumination\":\"%s\",\"sensor_number\":%s,\"event_id\":\"%s\"}",
                        timeStamp,
                        String.format("%.1f", bodyTemp),
                        motion,
                        String.format("%.2f", rumination),
                        sensorNo, eventId
                );

                System.out.println(sensorEvent);

                if (i % 2 == 0)
                    TimeUnit.MILLISECONDS.sleep(50);

                byte[] eventBytes = sensorEvent.getBytes(StandardCharsets.UTF_8);
//                writer.putRecord(eventBytes);
            }
        }
    }

    private static int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    private static float getRandomFloat(float min, float max) {
        return min + new Random().nextFloat() * (max - min);
    }

    private static List<String> getListFromCsv(String path) {
        List<String> listFromCsv = new ArrayList<>();

        try {
            CSVParser parser = new CSVParser(new FileReader(path), CSVFormat.DEFAULT );

            parser.getRecords().forEach(row -> {
                StringBuilder sb = new StringBuilder();
                row.iterator().forEachRemaining(item -> sb.append(item).append(", "));
                listFromCsv.add(sb.toString().trim().replaceAll(",+$", ""));
            });

        } catch (IOException e) {
            e.printStackTrace();
        }

        return listFromCsv;
    }

    private static KinesisProducer createProducer() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setRequestTimeout(60000)
                .setRecordMaxBufferedTime(15000)
                .setRegion("us-west-2");
        return new KinesisProducer(config);
    }
}
