package pt.inescid.gsd.ssb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import pt.inescid.gsd.cachemining.HTable;

public class Main {

    private enum SequenceType {
        COLUMN, ROW;
    };

    private static SequenceType sequenceType = SequenceType.ROW;

    private static final String[] TABLES = { "t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9" };
    private static final String[] FAMILIES = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
            "r", "s", "t", "u", "v", "w", "x", "y", "z" };
    private static final String[] QUALIFIERS = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" };

    private static final int MAX_ROWS = 1000;
    private static final int MAX_WAVES = 1000;
    private static final int MAX_SEQUENCES = 1;
    private static final int MIN_SEQUENCE_ITEMS = 3;
    private static final double FREQ_SEQUENCE_RATIO = 0.5;

    private static List<List<DataContainer>> sequences;

    private static HTablePool htablePool = null;

    private static Random random = new Random(100);

    private static HBaseAdmin hbaseAdmin;

    private static Map<String, HTable> htables = new HashMap<String, HTable>();

    private static boolean tablesCreated = false;

    private static void init() throws IOException {
        final Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "prosopon");
        try {
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("HBase is running!");
            hbaseAdmin = new HBaseAdmin(config);
            createTables();

            for (String table : TABLES)
                htables.put(table, new HTable(config, table));

            // htablePool = new HTablePool(config, 10);

            // htable.setAutoFlush(false);
            // htable.setWriteBufferSize(1024 * 1024 * 12);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    private static void createTables() throws IOException {
        if (!hbaseAdmin.tableExists(TABLES[0])) {
            for (String table : TABLES) {
                HTableDescriptor desc = new HTableDescriptor(table);
                for (String family : FAMILIES)
                    desc.addFamily(new HColumnDescriptor(family));
                hbaseAdmin.createTable(desc);
            }
            tablesCreated = true;
        }
    }

    private static String randomString(final int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char c = (char) (random.nextInt((int) (Character.MAX_VALUE)));
            sb.append(c);
        }
        return sb.toString();
    }

    private static void populate() throws IOException {
        if (!tablesCreated)
            return;
        System.out.println("Populating...");
        for (int row = 0; row < MAX_ROWS; row++) {
            Put put = new Put(Bytes.toBytes(String.valueOf(row)));
            for (String family : FAMILIES)
                for (String qualifier : QUALIFIERS)
                    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(randomString(random.nextInt(100))));

            for (String table : TABLES)
                htables.get(table).put(put);
        }
    }

    private static void loadFrequentSequences() {
        sequences = new ArrayList<List<DataContainer>>();

        for (int i = 0; i < MAX_SEQUENCES; i++) {
            int sequenceSize = MIN_SEQUENCE_ITEMS + random.nextInt(20);
            List<DataContainer> sequence = new ArrayList<DataContainer>(sequenceSize);

            if (sequenceType == SequenceType.COLUMN) {
                String row = String.valueOf(random.nextInt(MAX_ROWS));
                System.out.print("SEQ " + i + ": ");
                for (int j = 0; j < sequenceSize; j++) {
                    String table = TABLES[random.nextInt(TABLES.length)];
                    String family = FAMILIES[random.nextInt(FAMILIES.length)];
                    String qualifier = QUALIFIERS[random.nextInt(QUALIFIERS.length)];
                    sequence.add(new DataContainer(table, row, family, qualifier));
                    System.out.print(table + ":" + row + ":" + family + ":" + qualifier + " ");
                }
                System.out.println();

            } else if (sequenceType == SequenceType.ROW) {

                String table = TABLES[random.nextInt(TABLES.length)];
                String family = FAMILIES[random.nextInt(FAMILIES.length)];
                String qualifier = QUALIFIERS[random.nextInt(QUALIFIERS.length)];
                for (int j = 0; j < sequenceSize; j++) {
                    String row = String.valueOf(random.nextInt(MAX_ROWS));
                    sequence.add(new DataContainer(table, row, family, qualifier));
                    System.out.print(table + ":" + row + ":" + family + ":" + qualifier + " ");
                }
                System.out.println();
            }
            sequences.add(sequence);
        }
    }

    private static void runWorkload() throws IOException {

        Random random = new Random();
        for (int wave = 0; wave < MAX_WAVES; wave++) {

            htables.get(TABLES[0]).markTransaction();
            if (random.nextDouble() > FREQ_SEQUENCE_RATIO) {
                List<DataContainer> sequence = sequences.get(random.nextInt(sequences.size()));
                for (DataContainer dc : sequence) {
                    Get get = new Get(dc.getRow());
                    get.addColumn(dc.getFamily(), dc.getQualifier());
                    htables.get(Bytes.toString(dc.getTable())).get(get);
                }
            } else {
                int size = MIN_SEQUENCE_ITEMS + random.nextInt(20);
                for (int i = 0; i < size; i++) {

                    Get get = new Get(Bytes.toBytes(String.valueOf(random.nextInt(MAX_ROWS))));
                    String table = TABLES[random.nextInt(TABLES.length)];
                    String family = FAMILIES[random.nextInt(FAMILIES.length)];
                    String qualifier = QUALIFIERS[random.nextInt(QUALIFIERS.length)];
                    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                    htables.get(table).get(get);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        init();
        populate();
        loadFrequentSequences();
        long startTick = System.currentTimeMillis();
        runWorkload();
        long endTick = System.currentTimeMillis();

        System.out.println("Time taken: " + (endTick - startTick));

    }

    private static class DataContainer {
        private byte[] table;
        private byte[] row;
        private byte[] family;
        private byte[] qualifier;

        public DataContainer(String table, String row, String family, String qualifier) {
            this(Bytes.toBytes(table), Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier));
        }

        public DataContainer(byte[] table, byte[] row, byte[] family, byte[] qualifier) {
            this.table = table;
            this.row = row;
            this.family = family;
            this.qualifier = qualifier;
        }

        public byte[] getTable() {
            return table;
        }

        public void setTable(byte[] table) {
            this.table = table;
        }

        public byte[] getRow() {
            return row;
        }

        public void setRow(byte[] row) {
            this.row = row;
        }

        public byte[] getFamily() {
            return family;
        }

        public void setFamily(byte[] family) {
            this.family = family;
        }

        public byte[] getQualifier() {
            return qualifier;
        }

        public void setQualifier(byte[] qualifier) {
            this.qualifier = qualifier;
        }
    }
}
