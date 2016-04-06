package dsi.kvmodel.microarray;

/**
 * Created by sw1111 on 11/03/2016.
 */


import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MongoSQL {

    public static void insert (String studyname, String patientname, String annofilename, String datafilename, long cachesize) {
        {
            BufferedReader filein = null;
            BufferedReader annoIn = null;
            BufferedReader paIn = null;
            String line;
            StringTokenizer stin; // for deep token parse
            long count = 0;

            // connect to the local database server
            MongoClient mongoClient = new MongoClient("mongo-1", 27017);
            DB db = mongoClient.getDB("microarray");
            DBCollection coll = db.getCollection("data");
            BulkWriteOperation builder = coll.initializeOrderedBulkOperation();

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
                        BasicDBObject doc = new BasicDBObject("study", studyname)
                                .append("subject", paList.get(patientId))
                                .append("probeid", annoList.get(probeId))
                                .append("raw", raw)
                                .append("log", raw)
                                .append("zscore", raw);
                        builder.insert(doc);
                        probeId++;
                        count++;
                        if (count % cachesize == 0) {
                            System.out.println(count);
                            BulkWriteResult result = builder.execute();
                            builder = coll.initializeOrderedBulkOperation();
                            System.out.println("Ordered bulk write result : " + result);
                            putList.clear();
                        }
                    }
                    patientId++;
                }
                System.out.println("final count is " + count);
                BulkWriteResult result = builder.execute();
                System.out.println("Ordered bulk write result : " + result);
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
    }

    private class MultipleReader implements Runnable {
        private String filename;
        private List<String> studyList;

        public MultipleReader (List<String> studyList, String filename) throws IOException {
            this.filename = filename;
            this.studyList = studyList;
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

            List<String> queryList = new ArrayList<String>();

            for (int i = 0; i < 100; i++) {
                queryList.add(probeList.get(ThreadLocalRandom.current().nextInt(0, probeList.size())));
            }
            MongoClient mongoClient = new MongoClient("mongo-1", 27017);
            DB db = mongoClient.getDB("microarray");
            DBCollection coll = db.getCollection("data");
            BasicDBObject query = new BasicDBObject("probeid", new BasicDBObject("$in", queryList));
            DBCursor cursor = coll.find(query);
            long count = 0;
            try {
                while(cursor.hasNext()) {
                    cursor.next();
                    //System.out.println(cursor.next());
                }
            } finally {
                cursor.close();
            }
            System.out.println("finish");
        }
    }

    public void scheduler (List<String> study, String probeFileName, int threadNum) throws IOException {
        for (int i = 0; i < threadNum; i++) {
            Runnable r = new MultipleReader(study, probeFileName);
            Thread t = new Thread(r);
            t.start();
        }
    }

    public static void printHelp() {
        System.out.println("This class implement SQL model in a MongoDB");
        System.out.println("please input an argument");
        System.out.println("insert for inserting data into the table, parameter study name, patient file, annotation file, data file, cache size");
        System.out.println("search-probe for fetching all records, parameter probe file name, cache size, study1, study2, study3..");
    }

    public static void main(final String[] args) throws IOException {
        if (args.length < 1) {
            printHelp();
            return;
        }

        if (args[0].equals("insert")) {
            MongoSQL.insert(args[1], args[2], args[3], args[4], Long.parseLong(args[5]));
        } else if (args[0].equals("search-probe")) {
            long ts1 = System.currentTimeMillis();
            List<String> studyList = new ArrayList<String>();
            for (int i = 3; i < args.length; i++)
                studyList.add(args[i]);
            MongoSQL mongo = new MongoSQL();
            mongo.scheduler(studyList, args[1], Integer.parseInt(args[2]));
            long ts2 = System.currentTimeMillis();
            System.err.println("Total time is " + (ts2 - ts1));
        } else
            printHelp();
    }

    // CHECKSTYLE:OFF
    /**
     * Run this main method to see the output of this quick example.
     *
     * @throws UnknownHostException if it cannot connect to a MongoDB instance at localhost:27017
     */
    public static void backup() throws UnknownHostException {
        // connect to the local database server
        MongoClient mongoClient = new MongoClient("mongo-1", 27017);

        /*
        // Authenticate - optional
        MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password);
        MongoClient mongoClient = new MongoClient(new ServerAddress(), Arrays.asList(credential));
        */

        // get handle to "mydb"
        DB db = mongoClient.getDB("microarray");

        /* get a list of the collections in this database and print them out
        Set<String> collectionNames = db.getCollectionNames();
        for (final String s : collectionNames) {
            System.out.println(s);
        }*/

        // get a collection object to work with
        DBCollection coll = db.getCollection("data");

        // drop all the data in it
        //coll.drop();

        // make a document and insert it
        BasicDBObject doc = new BasicDBObject("study", "GSE24080")
                .append("subject", "79101")
                .append("probeid", "1007_at")
                .append("raw", 0.2934234234)
                .append("log", 0.2982738947)
                .append("zscore", 0.9234242234);

        coll.insert(doc);

        // get it (since it's the only one in there since we dropped the rest earlier on)
        DBObject myDoc = coll.findOne();
        System.out.println(myDoc);

        // now, lets add lots of little documents to the collection so we can explore queries and cursors
        for (int i = 0; i < 100; i++) {
            coll.insert(new BasicDBObject("study", "GSE24080")
                    .append("subject", "7910" + i)
                    .append("probeid", "1007_at")
                    .append("raw", 0.2934234234)
                    .append("log", 0.2982738947)
                    .append("zscore", 0.9234242234));
        }
        System.out.println("total # of documents after inserting 100 small ones (should be 101) " + coll.getCount());

        // lets get all the documents in the collection and print them out
        DBCursor cursor = coll.find();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // now use a query to get 1 document out
        BasicDBObject query = new BasicDBObject("subject", "79101");
        cursor = coll.find(query);
        System.out.println("print where subject = 79101");
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // $ Operators are represented as strings
        //query = new BasicDBObject("j", new BasicDBObject("$ne", 3))
        //        .append("k", new BasicDBObject("$gt", 10));
        List<String> subjectList = new ArrayList<String>();
        subjectList.add("79101");
        subjectList.add("79102");
        subjectList.add("79103");
        query = new BasicDBObject("subject", new BasicDBObject("$in", subjectList));

        cursor = coll.find(query);
        System.out.println("print where subject = 79101, 79102, 79103");
        try {
            while(cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        /* now use a range query to get a larger subset
        // find all where i > 50
        query = new BasicDBObject("i", new BasicDBObject("$gt", 50));
        cursor = coll.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }*/

        /* range query with multiple constraints
        query = new BasicDBObject("i", new BasicDBObject("$gt", 20).append("$lte", 30));
        cursor = coll.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }*/

        // Count all documents in a collection but take a maximum second to do so
        //coll.find().maxTime(1, SECONDS).count();

        // Bulk operations
        BulkWriteOperation builder = coll.initializeOrderedBulkOperation();
        builder.insert(new BasicDBObject("_id", 1));
        builder.insert(new BasicDBObject("_id", 2));
        builder.insert(new BasicDBObject("_id", 3));

        builder.find(new BasicDBObject("_id", 1)).updateOne(new BasicDBObject("$set", new BasicDBObject("x", 2)));
        builder.find(new BasicDBObject("_id", 2)).removeOne();
        builder.find(new BasicDBObject("_id", 3)).replaceOne(new BasicDBObject("_id", 3).append("x", 4));

        BulkWriteResult result = builder.execute();
        System.out.println("Ordered bulk write result : " + result);

        // Unordered bulk operation - no guarantee of order of operation
        builder = coll.initializeUnorderedBulkOperation();
        builder.find(new BasicDBObject("_id", 1)).removeOne();
        builder.find(new BasicDBObject("_id", 2)).removeOne();

        result = builder.execute();
        System.out.println("Ordered bulk write result : " + result);

        /* parallelScan
        ParallelScanOptions parallelScanOptions = ParallelScanOptions
                .builder()
                .numCursors(3)
                .batchSize(300)
                .build();

        List<Cursor> cursors = coll.parallelScan(parallelScanOptions);
        for (Cursor pCursor: cursors) {
            while (pCursor.hasNext()) {
                System.out.println(pCursor.next());
            }
        }*/

        // release resources
        //db.dropDatabase();
        mongoClient.close();
    }
    // CHECKSTYLE:ON
}
