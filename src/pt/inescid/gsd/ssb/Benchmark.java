package pt.inescid.gsd.ssb;

import ca.pfv.spmf.algorithms.sequentialpatterns.spam.AlgoVMSP;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import pt.inescid.gsd.cachemining.DataContainer;
import pt.inescid.gsd.cachemining.HTable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Benchmark {

    private enum SequenceType {
        COLUMN, ROW
    }

    private static final String statsFName = String.format("stats-benchmark-%d.csv", System.currentTimeMillis());

    private static final String accessesFNameMask = "accesses-%d.txt";

    private static final String STATS_HEADER = "timestamp,op,latency,runtime";

    private static final String[] TABLES = { "t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9" };
    private static final String[] FAMILIES = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
            "r", "s", "t", "u", "v", "w", "x", "y", "z" };
    private static final String[] QUALIFIERS = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" };

    private static final int SETS_OF_SEQUENCES = 3;

    private static Map<String, Integer> accessIndexes;

    private static final int MAX_ROWS = 1000;

    private static final double WAVE_CHUNK_PERCENTAGE = 0.1;

    private static List<List<List<DataContainer>>> sequences;

    // private static List<List<DataContainer>> extraSequences;

    private static Random random = new Random(100);

    private static HBaseAdmin hbaseAdmin;

    private static Map<String, HTable> htables = new HashMap<>();

    private static boolean tablesCreated = false;

    private static BufferedWriter statsF;

    private static BufferedWriter accessesF;

    private static String accessesFName;

    private static int sequencesSize;

    private static SequenceType sequenceType = SequenceType.ROW;

    private static int sequenceMinSize = 3;

    private static int sequenceMaxSize = 10;

    private static int blockSize;

    private static int zipfn = 100;

    private static double zipfe = 3;

    private static int waves;

    private static boolean outputAccesses = false;

    private static void init() {
        final Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "ginja-a4");
        try {
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("HBase is running!");
            hbaseAdmin = new HBaseAdmin(config);
            createTables();

            if (outputAccesses) {
                // build indexes
                accessIndexes = new HashMap<>();
                for (int i = 0; i < TABLES.length; i++) {
                    accessIndexes.put(TABLES[i], i);
                }
                for (int i = 0; i < FAMILIES.length; i++) {
                    accessIndexes.put(FAMILIES[i], i);
                }
                for (int i = 0; i < QUALIFIERS.length; i++) {
                    accessIndexes.put(QUALIFIERS[i], i);
                }
                accessesFName = String.format(accessesFNameMask, System.currentTimeMillis());
                accessesF = new BufferedWriter(new FileWriter(accessesFName));
            }

            statsF = new BufferedWriter(new FileWriter(statsFName));
            statsF.write(STATS_HEADER);
            statsF.newLine();

            generateFrequentSequences();
            // loadExtraSequences();
            for (String table : TABLES) {
                // htable.setAutoFlush(false);
                // htable.setWriteBufferSize(1024 * 1024 * 12);
                htables.put(table, new HTable(config, table, null));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    private static void loadExtraSequences() {
//        String line;
//        try {
//            BufferedReader in = new BufferedReader(new FileReader("result"));
//            while((line = in.readLine()) != null) {
//                String[] items = line.substring(0, line.indexOf("#")).replace("-1 ", "").split(" ");
//                List<DataContainer> seq = new ArrayList<>();
//                for(String item : items) {
//                    seq.add(decodeAccess(item.trim()));
//                }
//                extraSequences.add(seq);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

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

    private static void printSequences(List<List<DataContainer>> sequences) {
        for (List<DataContainer> sequence : sequences) {
            for (DataContainer dc : sequence) {
                System.out.print(encodeAccess(dc) + " ");
            }
            System.out.println();
        }
    }

    private static List<List<DataContainer>> generateBalancedSequenceTree(List<DataContainer> list, int depth,
            final int maxDepth, final String table, final String family, final String qualifier) {

        String row = String.valueOf(random.nextInt(MAX_ROWS));
        DataContainer dc = new DataContainer(table, row, family, qualifier);
        list.add(dc);

        List<List<DataContainer>> result = new ArrayList<>();
        if (depth == maxDepth) {
            result.add(list);
            return result;
        }

        List<List<DataContainer>> child1 = generateBalancedSequenceTree(new ArrayList(list), depth + 1,
                maxDepth, table, family, qualifier);
        List<List<DataContainer>> child2 =  generateBalancedSequenceTree(new ArrayList(list), depth + 1,
                maxDepth, table, family, qualifier);

        result.addAll(child1);
        result.addAll(child2);

        return result;
    }


    private static void generateFrequentSequences() {
        System.out.println("Generating frequent sequences...");

        sequences = new ArrayList<>();
//        extraSequences = new ArrayList<>();

        for(int s = 0; s < SETS_OF_SEQUENCES; s++) {

            List<List<DataContainer>> sequences = new ArrayList<>();
            Benchmark.sequences.add(sequences);

            for (int i = 0; i < sequencesSize; i++) {
                int sequenceSize = sequenceMinSize + random.nextInt((sequenceMaxSize - sequenceMinSize) + 1);
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
                    sequences.add(sequence);
//                    extraSequences.add(sequence);
                } else if (sequenceType == SequenceType.ROW) {
                    String table = TABLES[random.nextInt(TABLES.length)];
                    String family = FAMILIES[random.nextInt(FAMILIES.length)];
                    String qualifier = QUALIFIERS[random.nextInt(QUALIFIERS.length)];

                    List<List<DataContainer>> sequenceTree = generateBalancedSequenceTree(new ArrayList<DataContainer>(),
                            0, sequenceSize, table, family, qualifier);
                    sequences.addAll(sequenceTree);
//                    extraSequences.addAll(sequenceTree);

//                for (int j = 0; j < sequenceSize; j++) {
//                    String row = String.valueOf(random.nextInt(MAX_ROWS));
//                    DataContainer item = new DataContainer(table, row, family, qualifier);
//                    sequence.add(item);
//                    System.out.print(item + " ");
//                }
//                System.out.println();
                }
            }

            Collections.shuffle(sequences);
            // printSequences(sequences);
            System.out.println("Generated " + sequences.size() + " sequences");
        }
    }

    private static void runWorkload() throws IOException {
        System.out.println("Running workload...");
        Random random = new Random(100);

        // exponent is linked to the number of frequent sequences
        ZipfDistribution zipf = new ZipfDistribution(zipfn, zipfe);

        int index = 0;
        List<List<DataContainer>> sequences = Benchmark.sequences.get(index++);
        double wavesSet = Math.ceil((double)waves / (double)SETS_OF_SEQUENCES);
        int waveChunk = (int)(0.2 * wavesSet);

        for (int wave = 1; wave <= waves; wave++) {

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
                    if (outputAccesses) {
                        String value = encodeAccess(dc);
                        accessesF.write(value + " -1 ");
                    }
                }
            } else {
                int size = sequenceMinSize + random.nextInt((sequenceMaxSize - sequenceMinSize) + 1);
                for (int i = 0; i < size; i++) {

                    int rowInt = random.nextInt(MAX_ROWS);
                    Get get = new Get(Bytes.toBytes(String.valueOf(rowInt)));
                    int tableIndex = random.nextInt(TABLES.length);
                    String table = TABLES[tableIndex];
                    int familyIndex = random.nextInt(FAMILIES.length);
                    String family = FAMILIES[familyIndex];
                    int qualifierIndex = random.nextInt(QUALIFIERS.length);
                    String qualifier = QUALIFIERS[qualifierIndex];
                    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

                    long startTick = System.nanoTime();
                    htables.get(table).get(get);
                    long endTick = System.nanoTime();
                    long diff = endTick - startTick;

                    statsF.write(endTick + ",g," + diff + ",\n");
                    if (outputAccesses) {
                        String value = encodeAccess(rowInt, tableIndex, familyIndex, qualifierIndex);
                        accessesF.write(value + " -1 ");
                    }
                }
            }
            if (outputAccesses) {
                accessesF.write("-2\n");
            }
            if(wave % wavesSet == 0) {
                // TODO
                sequences = Benchmark.sequences.get(index++);
                if(outputAccesses) {
                    accessesF.close();
                    accessesFName = String.format(accessesFNameMask, System.currentTimeMillis());
                    accessesF = new BufferedWriter(new FileWriter(accessesFName));
                }
            } else if (wave % waveChunk == 0) {
                // TODO

                // run data mining
                AlgoVMSP algo = new AlgoVMSP();
                accessesF.flush();
                String resultFName = "result";
                algo.runAlgorithm(accessesFName, resultFName, 0.01);

                List<List<DataContainer>> seqs = new ArrayList<>();
                BufferedReader br = new BufferedReader(new FileReader(resultFName));
                String line;
                while((line = br.readLine()) != null) {
                    String[] items = line.split(" -1 ");
                    if(items.length <= 2) {
                        continue;
                    }

                    List<DataContainer> seq = new ArrayList<>();
                    // ignore last element of array
                    for(int i = 0; i < items.length - 1; i++) {
                        seq.add(decodeAccess(items[i]));
                    }
                    seqs.add(seq);
                }
                br.close();

                // update frequent sequences
                htables.values().iterator().next().updateSequences(seqs);
            }

//            try {
//                Thread.sleep(1);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

    private static String encodeAccess(DataContainer dc) {
        int tableIndex = accessIndexes.get(dc.getTableStr());
        int familyIndex = accessIndexes.get(dc.getFamilyStr());
        int qualifierIndex = accessIndexes.get(dc.getQualifierStr());
        return encodeAccess(Integer.parseInt(dc.getRowStr()), tableIndex, familyIndex, qualifierIndex);
    }

    private static String encodeAccess(int rowInt, int tableIndex, int familyIndex, int qualifierIndex) {
        // spmf only allows ints
        return 9 + String.format("%04d", rowInt) + tableIndex + String.format("%02d", familyIndex) + qualifierIndex;
    }

    private static DataContainer decodeAccess(String dcStr) {
        String row = dcStr.substring(1,5).replaceFirst("^0+(?!$)", "");
        int tableIndex = Integer.parseInt(dcStr.substring(5,6));
        int familyIndex = Integer.parseInt(dcStr.substring(6,8));
        int qualifierIndex = Integer.parseInt(dcStr.substring(8,9));

        return new DataContainer(TABLES[tableIndex], row, FAMILIES[familyIndex], QUALIFIERS[qualifierIndex]);
    }

    public static void main(String[] args) throws IOException {
        if(args.length < 8) {
            System.err.println("Usage: Benchmark <frequentSequencesSize> <sequenceType> <sequenceMinSize> "
                    + "<sequenceMaxSize> <blockSize> <zipfn> <zipfe> <waves> [<output-accesses>]");
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
        if (args.length > 8) {
            outputAccesses = Boolean.parseBoolean(args[8]);
        }

        init();
        populate();

        long startTick = System.currentTimeMillis();
        runWorkload();
        long endTick = System.currentTimeMillis();
        long diff = endTick - startTick;
        System.out.println("Time taken: " + diff);

        if(outputAccesses) {
            accessesF.close();
        }
        statsF.write(",,," + diff + "\n");
        statsF.close();
        // to close htable stats file
        for(HTable htable : htables.values()) {
            htable.close();
        }
        System.exit(0);
    }
}
