/**
 *
 * Molecular profiling based patient stratification plays a crucial role in clinical decision making, such as identification of disease subgroups and prediction of treatment responses of individual subjects. Many existing knowledge management systems like tranSMART enable scientists to do such analysis. But in the big data era, molecular profiling data size increases sharply due to new biological techniques, such as next generation sequencing. None of the existing systems work well whilst considering the three V features of big data (Volume, Variety, and Velocity). Massive scale data stores and collabourating data processing frameworks have been created to deal with the big data deluge. Hadoop Ecosystem, which includes HBase data store and MapReduce framework, has been worldly used for this purpose.
 *
 * Databases like Apache HBase and Google Bigtable can be modeled as Distributed Ordered Table (DOT). DOT horizontally partitions a table into regions and distributes regions to region servers by the Key. DOT further vertically partitions a region into groups (Family) and distributes Families to files (HFile). Multi-dimensional range queries on DOTs are fundamental requirements; however, none of existing data models work well for genetics while considering the three Vs. This thesis introduces a Collabourative Genomic Data Model (GCDM) to solve all these issues. GCDM creates three Collabourative Global Clustering Index Tables (CGCITs) for velocity and variety issues at the cost of limited extra volume. Microarray implementation of GCDM on HBase performed up to 10x faster than the popular SQL model on MySQL Cluster by using 1.5x larger disk space. SNP implementation of GCDM on HBase outperformed the popular SQL model on MySQL Cluster by up to 10 times at the cost of 3x larger volume. Raw sequence implementation of GCDM on HBase shows up to 10-fold velocity increasing by using 3xx larger volume.
 *
 *
 *
 */

package dsi.kvmodel.microarray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.UUID;

/**
 * Created by sw1111 on 02/02/2016.
 * Test SQL model performance in HBase
 *
 * Main table
 * __________________________________________________
 * _____________key_____________________|    value
 *   row key     |____column key________|
 * ______________|__family_|_qualifier__|____________
 *   id	         |  info   | study	    |
 *   	         |  info   | subject	|
 *   	         |  info   | probe	    |
 *   	         |  info   | raw	    |
 *   	         |  info   | log	    |
 *   	         |  info   | zscore	    |
 *
 *
 * Composite index table (study, subject)
 * __________________________________________________
 * _____________key_____________________|    value
 *   row key     |____column key________|
 * ______________|__family_|_qualifier__|____________
 * study:subject |  info   | probe	    |     id
 *
 *
 * Composite index table (study, probe)
 * __________________________________________________
 * _____________key_____________________|    value
 *   row key     |____column key________|
 * ______________|__family_|_qualifier__|____________
 * study:subject |  info   | probe	    |     id
 *
 */
public class HBaseSQL {
    private static final String COL_FAMILY_INFO = "info";
    private HTable MicroarraySQLTable;
    private HTable indexTable;

    public HBaseSQL(String table) throws IOException {
        Configuration config = HBaseConfiguration.create();
        MicroarraySQLTable = new HTable(config, table);
    }

    public HBaseSQL(String main, String index) throws IOException {
        Configuration config = HBaseConfiguration.create();
        MicroarraySQLTable = new HTable(config, main);
        indexTable = new HTable(config, index);
    }

    public static void createTable(String tablename) {
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
     *
     * @param studyname
     * @param patientname
     * @param annofilename
     * @param datafilename
     * @param cachesize
     */
    public void insertSubjectTable(String studyname, String patientname, String annofilename, String datafilename, long cachesize) {
        BufferedReader filein = null;
        BufferedReader annoIn = null;
        BufferedReader paIn   = null;
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
                annoList.add(line);
            }
            while ((line = paIn.readLine()) != null) {
                paList.add(line);
            }
            System.out.println("file " + datafilename);
            int patientId = 0;
            while ((line = filein.readLine()) != null) {
                stin = new StringTokenizer(line, ",");
                int probeId = 0;
                while (stin.hasMoreTokens()) {
                    String raw = stin.nextToken();
                    Put p = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                    p.add(Bytes.toBytes(COL_FAMILY_INFO),
                            Bytes.toBytes("study"), Bytes.toBytes(studyname));
                    p.add(Bytes.toBytes(COL_FAMILY_INFO),
                            Bytes.toBytes("subject"), Bytes.toBytes(paList.get(patientId)));
                    p.add(Bytes.toBytes(COL_FAMILY_INFO),
                            Bytes.toBytes("probeid"), Bytes.toBytes(annoList.get(probeId)));
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

    public void count(String startrow, String stoprow, int threshold, int cacheSize) throws IOException {
        Scan s = new Scan();
        s.setCacheBlocks(true);
        s.setCaching(cacheSize);
        s.setStartRow(Bytes.toBytes(startrow));
        s.setStopRow(Bytes.toBytes(stoprow));
        ResultScanner scanner = MicroarraySQLTable.getScanner(s);
        long count = 0;
        long kvCount = 0;
        try {
            long ts1 = System.currentTimeMillis();
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                if (count == threshold)
                    break;
                int psnum = 0;
                for (Cell kv : rr.rawCells()) {
                    if (psnum == 0 && count % cacheSize == 0)
                        // this count is 1 different from the count print
                        System.out.println(Bytes.toString(CellUtil.cloneRow(kv)));
                    psnum++;
                }
                System.out.println(psnum);
                kvCount += psnum;
                count++;
                if (count % cacheSize == 0)
                    System.out.println(count);
            }
            long ts2 = System.currentTimeMillis();
            System.out.println("Total time is " + (ts2 - ts1));
            System.out.println(kvCount);
        } finally {
            scanner.close();
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
                        System.out.println(Bytes.toString(CellUtil.cloneRow(kv)) + " : " +
                                Bytes.toString(CellUtil.cloneFamily(kv)) + ":" +
                                Bytes.toString(CellUtil.cloneQualifier(kv)) + " : " +
                                Bytes.toString(CellUtil.cloneValue(kv)));
//                        System.out.println(Bytes.toString(CellUtil.cloneQualifier(kv)));
                        psnum++;
                    }
                    count++;
                    if (count % cacheSize == 0)
                        System.out.println(count);
                }
                long ts2 = System.currentTimeMillis();
                System.out.println("Total time is " + (ts2 - ts1));
            } finally {
                scanner.close();
            }
        } catch (Exception ee) {

        }
    }

    /**
     System.out.println("* __________________________________________________");
     System.out.println("* _____________key_____________________|            ");
     System.out.println("*   row key     |____column key________|    value   ");
     System.out.println("* ______________|__family_|_qualifier__|____________");
     System.out.println("*  col1:col2    |  info   | col3       |     id     ");
     * Create composite key secondary index
     * @param col1
     * @param col2 col1 and col2 are composite row key.
     * @param col3 family key
     * @param cacheSize
     */
    public void createIndex(String col1, String col2, String col3, int cacheSize) {
        List<Put> putList = new ArrayList<Put>();
        try {
            Scan s = new Scan();
            s.setCacheBlocks(true);
            s.setCaching(cacheSize);
            s.setStartRow(Bytes.toBytes("."));
            s.setStopRow(Bytes.toBytes("}"));
            ResultScanner scanner = MicroarraySQLTable.getScanner(s);
            long count = 0;
            try {
                long ts1 = System.currentTimeMillis();
                for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                    String id = null;
                    String vCol1 = null;
                    String vCol2 = null;
                    String vCol3 = null;
                    int psnum = 0;
                    for (Cell kv : rr.rawCells()) {
                        if (psnum == 0)
                            id = Bytes.toString(CellUtil.cloneRow(kv));
                        String col = Bytes.toString(CellUtil.cloneQualifier(kv));
                        if (col.equals(col1))
                            vCol1 = Bytes.toString(CellUtil.cloneValue(kv));
                        else if (col.equals(col2))
                            vCol2 = Bytes.toString(CellUtil.cloneValue(kv));
                        else if (col.equals(col3))
                            vCol3 = Bytes.toString(CellUtil.cloneValue(kv));
                        psnum++;
                    }
                    if (psnum != 6)
                        System.err.println("!!!!!!!!!!!!!!!!Inconsistent record found!!!!!!!!!!!!!!");

                    Put p = new Put(Bytes.toBytes(vCol1 + ":" + vCol2));
                    p.add(Bytes.toBytes(COL_FAMILY_INFO),
                            Bytes.toBytes(vCol3), Bytes.toBytes(id));
                    putList.add(p);

                    count++;
                    if (count % cacheSize == 0) {
                        System.out.println(count + "\t" + id);
                        indexTable.put(putList);
                        putList.clear();
                    }
                }
                indexTable.put(putList);
                putList.clear();
                long ts2 = System.currentTimeMillis();
                System.out.println("Total time is " + (ts2 - ts1));
                System.out.println(count);
            } finally {
                scanner.close();
            }
        } catch (Exception ee) {

        }
    }

    /**
     * @date 08/02/2016
     * load each line as a string and save them as a list
     * @param filename
     * @return
     * @throws IOException
     */
    private List<String> getSubjectList(String filename) throws IOException {
        BufferedReader br = null;
        String line = null;
        List<String> subjectList = new ArrayList<String>();
        br = new BufferedReader(new FileReader(new File(filename)));
        while ((line = br.readLine()) != null) {
            subjectList.add(line);
        }
        return subjectList;
    }

    /**
     * return
     * @param study
     * @param subjectFile
     * @param cacheSize
     * @throws IOException
     */
    public void searchBySubject(String study, String subjectFile, int cacheSize) throws IOException {
        long ts1 = System.currentTimeMillis();
        ArrayList<Get> getIndexList = new ArrayList<Get>();
        ArrayList<Get> getDataList = new ArrayList<Get>();
        List<String> paList = getSubjectList(subjectFile);
        for (String subject : paList) {
            Get get = new Get(Bytes.toBytes(study + ":" + subject));
            getIndexList.add(get);
        }
        Result[] results = indexTable.get(getIndexList);
        for (int i = 0; i < results.length; i++) {
            for (Cell kv : results[i].rawCells()) {
                Get get = new Get(CellUtil.cloneValue(kv));
                getDataList.add(get);
                if (getDataList.size() == cacheSize) {
                    Result[] resultData = MicroarraySQLTable.get(getDataList);
                    for (int j = 0; j < resultData.length; j++) {
                        int psnum = 0;
                        for (Cell kvs : resultData[j].rawCells()) {
                            psnum++;
                        }
                        System.out.println("should be 6 =" + psnum);
                    }
                }
            }
        }
        long ts2 = System.currentTimeMillis();
        System.out.println("Total time is " + (ts2 - ts1));
    }

    /**
     * orScan uses or condition on multiple singleColumnFilter.The function has limited usage.
     *
     * NOT IN USE
     *
     * @param startrow
     * @param stoprow
     * @param filterCol
     * @param filterFile
     * @param threshold
     * @param cacheSize
     * @throws IOException
     */
    public void orScan(String startrow, String stoprow, String filterCol, String filterFile, int threshold, int cacheSize) throws IOException {
        BufferedReader filein = null;
        String line = null;
        List<String> filterList = new ArrayList<String>();
        try {
            filein = new BufferedReader(new FileReader(filterFile));
            while ((line = filein.readLine()) != null) {
                filterList.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                filein.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for(String filterString : filterList) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("info"),
                    Bytes.toBytes(filterCol),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(filterString)
            );
            filters.addFilter(filter);
        }

        Scan s = new Scan();
        s.setCacheBlocks(true);
        s.setCaching(cacheSize);
        s.setStartRow(Bytes.toBytes("."));
        s.setStopRow(Bytes.toBytes("}"));
        s.setFilter(filters);

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
                if (count % cacheSize == 0)
                    System.out.println(count);
            }
            long ts2 = System.currentTimeMillis();
            System.out.println("Total time is " + (ts2 - ts1));
            System.out.println("Total number is " + count);
        } finally {
            scanner.close();
        }
    }

    public static void printHelp() {
        System.out.println("This class implement SQL model in a HBase");
        System.out.println("please input an argument");
        System.out.println("create for creating a new table, parameter table name");
        System.out.println("insert for inserting data into the table, parameter table name, study name, patient file, annotation file, data file, cache size");
        System.out.println("scan for read data from the table, parameter table, start key, end key, threshold, data file, cache size");
        System.out.println("count for count records from the table, parameter table, start key, end key, threshold, data file, cache size");
        System.out.println("index for generating secondary index from main table to index table, parameter main table, index table, column 1, column 2, column 3, cache size");
        System.out.println("* __________________________________________________");
        System.out.println("* _____________key_____________________|            ");
        System.out.println("*   row key     |____column key________|    value   ");
        System.out.println("* ______________|__family_|_qualifier__|____________");
        System.out.println("*  col1:col2    |  info   | col3       |     id     ");
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            printHelp();
            return;
        }

        if (args[0].equals("create")) {
            HBaseSQL.createTable(args[1]);
        } else if (args[0].equals("insert")) {
            HBaseSQL sql = new HBaseSQL(args[1]);
            sql.insertSubjectTable(args[2], args[3], args[4], args[5], Long.parseLong(args[6]));
        } else if (args[0].equals("scan")) {
            HBaseSQL sql = new HBaseSQL(args[1]);
            sql.scan(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        } else if (args[0].equals("count")) {
            HBaseSQL sql = new HBaseSQL(args[1]);
            sql.count(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        } else if (args[0].equals("index")) {
            HBaseSQL sql = new HBaseSQL(args[1], args[2]);
            sql.createIndex(args[3], args[4], args[5], Integer.parseInt(args[6]));
        } else
            printHelp();
    }
}
