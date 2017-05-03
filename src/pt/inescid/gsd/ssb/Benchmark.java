package pt.inescid.gsd.ssb;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import pt.inescid.gsd.cachemining.DataContainer;
import pt.inescid.gsd.cachemining.HTable;

public class Benchmark {

    private enum SequenceType {
        COLUMN, ROW
    }

    private static final String statsFName = String.format("stats-benchmark-%d.csv", System.currentTimeMillis());

    private static final String STATS_HEADER = "timestamp,op,latency,runtime";

    private static final String[] TABLES = { "t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9" };
    private static final String[] FAMILIES = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
            "r", "s", "t", "u", "v", "w", "x", "y", "z" };
    private static final String[] QUALIFIERS = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" };

    private static final int MAX_ROWS = 1000;

    private static List<List<DataContainer>> sequences;

    private static Random random = new Random(100);

    private static HBaseAdmin hbaseAdmin;

    private static Map<String, HTable> htables = new HashMap<>();

    private static boolean tablesCreated = false;

    private static BufferedWriter statsF;

    private static int sequencesSize;

    private static SequenceType sequenceType = SequenceType.ROW;

    private static int sequenceMinSize = 3;

    private static int sequenceMaxSize = 10;

    private static int blockSize;

    private static int zipfn = 100;

    private static double zipfe = 3;

    private static int waves;

    private static void init() throws IOException {
        final Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "ginja-a4");
        try {
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("HBase is running!");
            hbaseAdmin = new HBaseAdmin(config);
            createTables();

            for (String table : TABLES) {
                // htable.setAutoFlush(false);
                // htable.setWriteBufferSize(1024 * 1024 * 12);
                htables.put(table, new HTable(config, table, sequences));
            }

            statsF = new BufferedWriter(new FileWriter(statsFName));
            statsF.write(STATS_HEADER);
            statsF.newLine();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTables() throws IOException {
        if (!hbaseAdmin.tableExists(TABLES[0])) {
            System.out.println("Creating tables...");
            for (String table : TABLES) {
                HTableDescriptor desc = new HTableDescriptor(table);
                for (String family : FAMILIES)
                    desc.addFamily(new HColumnDescriptor(family));
                hbaseAdmin.createTable(desc);
            }
            tablesCreated = true;
        }
    }

    private static void populate() throws IOException {
        if (!tablesCreated)
            return;

        byte[] block = new byte[blockSize];
        random.nextBytes(block);

        System.out.println("Populating...");
        for (int row = 0; row < MAX_ROWS; row++) {
            Put put = new Put(Bytes.toBytes(String.valueOf(row)));
            for (String family : FAMILIES) {
                for (String qualifier : QUALIFIERS) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), block);
                }
            }

            for (String table : TABLES) {
                htables.get(table).put(put);
            }
        }
    }

    private static void generateFrequentSequences() {
        System.out.println("Generating frequent sequences...");

        sequences = new ArrayList<>(sequencesSize);

        for (int i = 0; i < sequencesSize; i++) {
            int sequenceSize = sequenceMinSize + random.nextInt(sequenceMaxSize);
            List<DataContainer> sequence = new ArrayList<>(sequenceSize);

            if (sequenceType == SequenceType.COLUMN) {
                String row = String.valueOf(random.nextInt(MAX_ROWS));
                System.out.print("SEQ " + i + ": ");
                for (int j = 0; j < sequenceSize; j++) {
                    String table = TABLES[random.nextInt(TABLES.length)];
                    String family = FAMILIES[random.nextInt(FAMILIES.length)];
                    String qualifier = QUALIFIERS[random.nextInt(QUALIFIERS.length)];
                    DataContainer item = new DataContainer(table, row, family, qualifier);
                    sequence.add(item);
                    System.out.print(item + " ");
                }
                System.out.println();

            } else if (sequenceType == SequenceType.ROW) {

                String table = TABLES[random.nextInt(TABLES.length)];
                String family = FAMILIES[random.nextInt(FAMILIES.length)];
                String qualifier = QUALIFIERS[random.nextInt(QUALIFIERS.length)];
                for (int j = 0; j < sequenceSize; j++) {
                    String row = String.valueOf(random.nextInt(MAX_ROWS));
                    DataContainer item = new DataContainer(table, row, family, qualifier);
                    sequence.add(item);
                    System.out.print(item + " ");
                }
                System.out.println();
            }

            sequences.add(sequence);
        }
    }

    private static void runWorkload() throws IOException {

        System.out.println("Running workload...");
        Random random = new Random(100);

        // exponent is linked to number of frequent sequences
        ZipfDistribution zipf = new ZipfDistribution(zipfn, zipfe);

        for (int wave = 0; wave < waves; wave++) {

            // htables.get(TABLES[0]).markTransaction();
            int sample = zipf.sample() - 1;
            if (sample < sequences.size()) {
                List<DataContainer> sequence = sequences.get(sample);
                for (DataContainer dc : sequence) {
                    Get get = new Get(dc.getRow());
                    get.addColumn(dc.getFamily(), dc.getQualifier());

                    long startTick = System.nanoTime();
                    htables.get(dc.getTableStr()).get(get);
                    long endTick = System.nanoTime();
                    long diff = endTick - startTick;

                    statsF.write(endTick + ",g," + diff + ",\n");
                }
            } else {
                int size = sequenceMinSize + random.nextInt(sequenceMaxSize);
                for (int i = 0; i < size; i++) {

                    // TODO: measure request latency and throughput
                    Get get = new Get(Bytes.toBytes(String.valueOf(random.nextInt(MAX_ROWS))));
                    String table = TABLES[random.nextInt(TABLES.length)];
                    String family = FAMILIES[random.nextInt(FAMILIES.length)];
                    String qualifier = QUALIFIERS[random.nextInt(QUALIFIERS.length)];
                    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

                    long startTick = System.nanoTime();
                    htables.get(table).get(get);
                    long endTick = System.nanoTime();
                    long diff = endTick - startTick;

                    statsF.write(endTick + ",g," + diff + ",\n");
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 8) {
            System.err.println("Usage: Benchmark <frequentSequencesSize> <sequenceType> <sequenceMinSize> "
                    + "<sequenceMaxSize> <blockSize> <zipfn> <zipfe> <waves>");
            System.exit(1);
        }
        sequencesSize = Integer.parseInt(args[0]);
        sequenceType = SequenceType.valueOf(args[1]);
        sequenceMinSize = Integer.parseInt(args[2]);
        sequenceMaxSize = Integer.parseInt(args[3]);
        blockSize = Integer.parseInt(args[4]);
        zipfn = Integer.parseInt(args[5]);
        zipfe = Double.parseDouble(args[6]);
        waves = Integer.parseInt(args[7]);

        generateFrequentSequences();
        init();
        populate();
        long startTick = System.currentTimeMillis();
        runWorkload();
        long endTick = System.currentTimeMillis();
        long diff = endTick - startTick;
        System.out.println("Time taken: " + diff);
        statsF.write(",,," + diff + "\n");
        statsF.close();

        // to close htable stats file
        for(HTable htable : htables.values()) {
            htable.close();
        }
    }
}
