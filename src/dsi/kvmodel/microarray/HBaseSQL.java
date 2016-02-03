/**
 *
 * Molecular profiling based patient stratification plays a crucial role in clinical decision making, such as identification of disease subgroups and prediction of treatment responses of individual subjects. Many existing knowledge management systems like tranSMART enable scientists to do such analysis. But in the big data era, molecular profiling data size increases sharply due to new biological techniques, such as next generation sequencing. None of the existing systems work well whilst considering the three V features of big data (Volume, Variety, and Velocity). Massive scale data stores and collabourating data processing frameworks have been created to deal with the big data deluge. Hadoop Ecosystem, which includes HBase data store and MapReduce framework, has been worldly used for this purpose.
 *
 * Databases like Apache HBase and Google Bigtable can be modeled as Distributed Ordered Table (DOT). DOT horizontally partitions a table into regions and distributes regions to region servers by the Key. DOT further vertically partitions a region into groups (Family) and distributes Families to files (HFile). Multi-dimensional range queries on DOTs are fundamental requirements; however, none of existing data models work well for genetics while considering the three Vs. This thesis introduces a Genomic Collabourating Data Model (GCDM) to solve all these issues. GCDM creates three Collabourating Global Clustering Index Tables (CGCITs) for velocity and variety issues at the cost of limited extra volume. Microarray implementation of GCDM on HBase performed up to 10x faster than the popular SQL model on MySQL Cluster by using 1.5x larger disk space. SNP implementation of GCDM on HBase outperformed the popular SQL model on MySQL Cluster by up to 10 times at the cost of 3x larger volume. Raw sequence implementation of GCDM on HBase shows up to 10-fold velocity increasing by using 3xx larger volume.
 *
 */

package dsi.kvmodel.microarray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import java.util.UUID;

/**
 * Created by sw1111 on 02/02/2016.
 * Test SQL model performance in HBase
 * __________________________________________________
 * _____________key_____________________|    value
 *   row key     |____column key________|
 * ______________|__family_|_qualifier__|____________
 *   1	         |  info   | study	    |
 *   2	         |  info   | subject	|
 *   3	         |  info   | probe	    |
 *   4	         |  info   | raw	    |
 *   5	         |  info   | log	    |
 *   6	         |  info   | zscore	    |
 */
public class HBaseSQL {
    private static final String COL_FAMILY_INFO = "info";
    private HTable MicroarraySQLTable;

    public HBaseSQL(String table) throws IOException {
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hadmin = new HBaseAdmin(config);
        if (hadmin.tableExists(table)) {
            MicroarraySQLTable = new HTable(config, table);
        }
    }

    public static void init(String tablename) {
        // create the microarray table
        try {
            Configuration config = HBaseConfiguration.create();
            HBaseAdmin hadmin = new HBaseAdmin(config);
            HTableDescriptor microarrayTableDesc = new HTableDescriptor(
                    TableName.valueOf(tablename));
            HColumnDescriptor infoColDesc = new HColumnDescriptor(COL_FAMILY_INFO);
            microarrayTableDesc.addFamily(infoColDesc);
            hadmin.createTable(microarrayTableDesc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param studyname
     * @param patientname
     * @param annofilename
     * @param datafilename
     * @param cachesize
     */
    public void insertSubjectTable(String studyname, String patientname, String annofilename, String datafilename, long cachesize) {
        BufferedReader filein = null;
        BufferedReader annoIn = null;
        BufferedReader paIn = null;
        String line;
        StringTokenizer stin; // for deep token parse
        long count = 0;
        try {
            filein = new BufferedReader(new FileReader(datafilename));
            annoIn = new BufferedReader(new FileReader(annofilename));
            paIn = new BufferedReader(new FileReader(patientname));
            List<String> annoList = new ArrayList<String>();
            List<String> paList = new ArrayList<String>();
            List<Put> putList = new ArrayList<Put>();
            while ((line = annoIn.readLine()) != null) {
                annoList.add(line.substring(1, line.length() - 1));
            }
            while ((line = paIn.readLine()) != null) {
                paList.add(line);
            }
            System.out.println("file " + datafilename);
            int patientId = 0;
            int autoID = 0;
            while ((line = filein.readLine()) != null) {
                stin = new StringTokenizer(line, ",");
                int probeId = 0;
                while (stin.hasMoreTokens()) {
                    String raw = stin.nextToken();
                    UUID idOne = UUID.randomUUID();
                    Put p = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                    p.add(Bytes.toBytes(COL_FAMILY_INFO),
                            Bytes.toBytes("study"), Bytes.toBytes(studyname));
                    p.add(Bytes.toBytes(COL_FAMILY_INFO),
                            Bytes.toBytes("subject"), Bytes.toBytes(paList.get(patientId)));
                    p.add(Bytes.toBytes(COL_FAMILY_INFO),
                            Bytes.toBytes("probeid"), Bytes.toBytes(probeId));
                    p.add(Bytes.toBytes(COL_FAMILY_INFO),
                            Bytes.toBytes("raw"), Bytes.toBytes(raw));
                    p.add(Bytes.toBytes(COL_FAMILY_INFO),
                            Bytes.toBytes("log"), Bytes.toBytes(Math.log(Double.parseDouble(raw)) + ""));
                    p.add(Bytes.toBytes(COL_FAMILY_INFO),
                            Bytes.toBytes("zscore"), Bytes.toBytes(raw));
                    putList.add(p);
                    probeId++;
                    count++;
                    if (count % cachesize == 0) {
                        System.out.println(count);
                        MicroarraySQLTable.put(putList);
                        putList.clear();
                    }
                }
                patientId++;
            }
            System.out.println("final count is " + count);
            MicroarraySQLTable.put(putList);
            putList.clear();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                filein.close();
                annoIn.close();
                paIn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void scan(String startrow, String stoprow, int threshold, int cacheSize) {
        try {
            Scan s = new Scan();
            s.setCacheBlocks(true);
            s.setCaching(cacheSize);
            s.setStartRow(Bytes.toBytes(startrow));
            s.setStopRow(Bytes.toBytes(stoprow));
            ResultScanner scanner = MicroarraySQLTable.getScanner(s);
            long count = 0;
            try {
                long ts1 = System.currentTimeMillis();
                for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                    if (count == threshold)
                        break;
                    int psnum = 0;
                    for (Cell kv : rr.rawCells()) {
                        psnum++;
                    }
                    count++;
                }
                long ts2 = System.currentTimeMillis();
                System.out.println("Total time is " + (ts2 - ts1));
            } finally {
                scanner.close();
            }
        } catch (Exception ee) {

        }
    }

    public static void printHelp() {
        System.out.println("This class implement SQL model in a HBase");
        System.out.println("please input an argument");
        System.out.println("init for create a new table, parameter table name");
        System.out.println("insert for insert data into the table, parameter studyname, patient file, annotation file, data file, cachesize");
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            printHelp();
            return;
        }

        if (args[0].equals("init")) {
            HBaseSQL.init(args[1]);
        } else if (args[0].equals("insert")) {
            HBaseSQL sql = new HBaseSQL(args[1]);
            sql.insertSubjectTable(args[2], args[3], args[4], args[5], Long.parseLong(args[6]));
        } else if (args[0].equals("scan")) {
            HBaseSQL sql = new HBaseSQL(args[1]);
            sql.scan(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        } else
            printHelp();
    }
}

