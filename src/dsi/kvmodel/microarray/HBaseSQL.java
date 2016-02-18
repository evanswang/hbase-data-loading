/**
 * Created by sw1111 on 02/02/2016.
 *
 * Experiment principles:
 * 1. We controlled the virtual machine (VM) number, cpu core number, memory size, disk volume. For standalone implementations, we gave all the cpu, memory and disk resources to a single VM. For each distributed implementations, we gave them the same cluster, where same number of VM are used and the architecture in each VM. The cluster can fulfill the requirements of all distributed implementations and be re-used to deploy different implementations.
 * 2. The target is to get the highest throughput in a clean environment.Potential influence on performance evaluation accuracy: VM Hypervisor I/O schedule behaviours and internal network I/O noise can significantly affect the performance of distributed implementation. The workers in distributed implementation need to synchronize and collabourate with each other through network communications. These activities generate large quantities of I/O requests through network.
 *
 * Discuss: what may influence the performance? Internal 1. Internal communication, such as heartbeats and schedules, synchronizes workers and distributes tasks within quite short interval. For example, the HeartbeatIntervalDbApi in MySQL Cluster is 1.5 seconds by default; while HBase hbase.cells.scanned.per.heartbeat.check interval is around 10 seconds. These internal synchronization may disturb client data query requests. 2. Loggers record database behaviour in memory before any action actually happens and flush the information into permanent storage periodically. If any worker in a cluster is much busier than others, the Logger in this work may flush more often than others. Loggers in different workers record the corresponding worker activities separately in different time. Some Logger may suffer from timeout during I/O busy time. The server will throw an exception for the timeout and restart the Logger. During this period, the server cannot response any requests. Some Logger use local storage as permanent storage media and others may use remote storage. The former one will cause local disk I/O requests and the latter one will lead to network I/O requests. 3. Data location may be changed from time to time. For example, each time regions may be assigned to different region servers. 4. VM hypervisor I/O scheduler. The VM hypervisor, such as IC Cloud and OpenStack, controlled all the guest VMs in a physical server. Any VM requests are handled by the hypervisor and transferred to physical systems. If other VMs share the physical machine with the experimental VMs, requests from all VMs will be sent to the same queue. The same request in a VM may be executed for a longer time if other VMs send many requests at that moment.
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
import java.util.concurrent.ThreadLocalRandom;

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
        //
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
        try {
            br = new BufferedReader(new FileReader(new File(filename)));
            while ((line = br.readLine()) != null) {
                subjectList.add(line);
            }
        } finally {
            br.close();
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
        ArrayList<Get> getDataList = new ArrayList<Get>();
        List<String> paList = getSubjectList(subjectFile);
        long count = 0;
        for (String subject : paList) {
            Get get = new Get(Bytes.toBytes(study + ":" + subject));
            Result result = indexTable.get(get);

            for (Cell kv : result.rawCells()) {
                Get getData = new Get(CellUtil.cloneValue(kv));
                getDataList.add(getData);
                if (getDataList.size() == cacheSize) {
                    count += get(getDataList);
                    getDataList.clear();
                    System.out.println(count);
                }
            }
            if (getDataList.size() == cacheSize) {
                count += get(getDataList);
                getDataList.clear();
                System.out.println(count);
            }
        }
        count += get(getDataList);
        getDataList.clear();
        System.out.println(count);

        long ts2 = System.currentTimeMillis();
        System.out.println("Total time is " + (ts2 - ts1));
        System.out.println("Total record number is " + count);
    }

    private long get(List<Get> getDataList) throws IOException {
        long count = 0;
        Result[] resultData = MicroarraySQLTable.get(getDataList);
        for (int j = 0; j < resultData.length; j++) {
            count++;
            for (Cell kvs : resultData[j].rawCells()) {
//                System.out.println(Bytes.toString(CellUtil.cloneRow(kvs)) + " : " +
//                        Bytes.toString(CellUtil.cloneFamily(kvs)) + " : " +
//                        Bytes.toString(CellUtil.cloneQualifier(kvs)) + " : " +
//                        Bytes.toString(CellUtil.cloneValue(kvs)));
            }
        }
        return count;
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

    public void scheduler (String study, String probeFileName, int threadNum) {
        for (int i = 0; i < threadNum; i++) {
            Runnable r = new MultipleReader(indexTable, MicroarraySQLTable, study, probeFileName);
            Thread t = new Thread(r);
            t.start();
        }
    }

    private class MultipleReader implements Runnable {
        private HTable indexTable;
        private HTable msqlTable;
        private String filename;
        private String study;
        public MultipleReader (HTable indexTable, HTable msqlTable, String study, String filename) {
            this.indexTable = indexTable;
            this.msqlTable = msqlTable;
            this.filename = filename;
            this.study = study;
        }
        public void run() {
            long ts1 = System.currentTimeMillis();
            List<String> probeList = new ArrayList<String>();
            List<Get> getList = new ArrayList<Get>();
            List<Get> getDataList = new ArrayList<Get>();
            BufferedReader br = null;
            String str = null;
            try {
                // read all probe list
                br = new BufferedReader(new FileReader(new File(filename)));
                while ((str = br.readLine()) != null) {
                    probeList.add(str);
                }
            } catch (FileNotFoundException e1) {
                e1.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            for (int i = 0; i < 100; i++) {
                Get get = new Get(Bytes.toBytes(study + ":" + probeList.get(ThreadLocalRandom.current().nextInt(0, probeList.size() + 1))));
                get.addFamily(Bytes.toBytes(COL_FAMILY_INFO));
                getList.add(get);
            }

            try {
                // search index table to get all ids.
                int psnum = 0;
                Result[] results = indexTable.get(getList);
                for (int i = 0; i < results.length; i++)
                    for (Cell kv : results[i].rawCells()) {
                        psnum++;
                        // each kv represents a column
                        //System.out.println(Bytes.toString(CellUtil.cloneRow(kv)));
                        //System.out.println(Bytes.toString(CellUtil.cloneFamily(kv)));
                        //System.out.println(Bytes.toString(CellUtil.cloneQualifier(kv)));
                        //System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
                        getDataList.add(new Get(CellUtil.cloneValue(kv)));
                    }
                long ts2 = System.currentTimeMillis();
                System.out.println("total number is " + psnum
                        + ". execute time is " + (ts2 - ts1) + ". end time is "
                        + ts2);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                // search data table to get all data.
                int psnum = 0;
                Result[] results = msqlTable.get(getDataList);
                for (int i = 0; i < results.length; i++)
                    for (Cell kv : results[i].rawCells()) {
                        psnum++;
                    }
                long ts2 = System.currentTimeMillis();
                System.out.println("total number is " + psnum
                        + ". execute time is " + (ts2 - ts1) + ". end time is "
                        + ts2);
            } catch (IOException e) {
                e.printStackTrace();
            }
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
        System.out.println("search-subject for fetching all records from the main table using the index table, parameter main table, index table, study name, file name, cache size");
        System.out.println("search-probe for fetching all records from the main table using the index table, parameter main table, index table, study name, file name, cache size");
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
        } else if (args[0].equals("search-subject")) {
            HBaseSQL sql = new HBaseSQL(args[1], args[2]);
            sql.searchBySubject(args[3], args[4], Integer.parseInt(args[5]));
        } else if (args[0].equals("search-probe")) {
            // TODO test
            HBaseSQL sql = new HBaseSQL(args[1], args[2]);
            sql.scheduler(args[3], args[4], Integer.parseInt(args[5]));
        } else
            printHelp();
    }
}



