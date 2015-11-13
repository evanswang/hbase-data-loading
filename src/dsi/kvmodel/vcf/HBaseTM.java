package dsi.kvmodel.vcf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
public class HBaseTM extends HBaseSNP {
	
	public HBaseTM(String tablename) throws IOException {
		super(tablename);
	}
	
	public static void init(String tablename) {
		System.out.println("This is the override init method ***************");
		// create the microarray table
		try {
			Configuration config = HBaseConfiguration.create();
			HBaseAdmin hadmin = new HBaseAdmin(config);
			HTableDescriptor mainTableDesc = new HTableDescriptor(tablename);
			HTableDescriptor subindTableDesc = new HTableDescriptor(tablename
					+ "-subject.index");
			HTableDescriptor infoTableDesc = new HTableDescriptor(tablename
					+ "-subject.info");
			HTableDescriptor posTableDesc = new HTableDescriptor(tablename
					+ "-pos.index");

			HColumnDescriptor infoColDesc = new HColumnDescriptor(
					COL_FAMILY_INFO);
			HColumnDescriptor subColDesc = new HColumnDescriptor(
					COL_FAMILY_SUBJECT);
			HColumnDescriptor posColDesc = new HColumnDescriptor(
					COL_FAMILY_POSITION);

			if (!hadmin.tableExists(tablename)) {
				mainTableDesc.addFamily(infoColDesc);
				mainTableDesc.addFamily(subColDesc);
				hadmin.createTable(mainTableDesc);
			}

			if (!hadmin.tableExists(tablename + "-subject.index")) {
				subindTableDesc.addFamily(posColDesc);
				hadmin.createTable(subindTableDesc);
			}

			if (!hadmin.tableExists(tablename + "-subject.info")) {
				infoTableDesc.addFamily(infoColDesc);
				hadmin.createTable(infoTableDesc);
			}

			if (!hadmin.tableExists(tablename + "-pos.index")) {
				posTableDesc.addFamily(infoColDesc);
				posTableDesc.addFamily(subColDesc);
				hadmin.createTable(posTableDesc);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void insertSubIndSNP(String trial, String subject) {
		insertSubIndSNP(trial, subject, 1000);
	}
	
	public void insertSubIndSNP(String trial, String subject, long cachesize) {

		BufferedReader br = null;
		String str = null;
		System.out.println("subject is " + subject);
		long ts1 = System.currentTimeMillis();
		System.out.println("start inserting SubInd table at " + ts1);

		int count = 0;
		List<Put> putList = new ArrayList<Put>();
		try {				
			br = new BufferedReader(new FileReader(new File(subject)));
			while ((str = br.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(str, "\t");
				String PATIENTID= tokenizer.nextToken(); // not id, but a name. not used
				String SAMPLETYPE= tokenizer.nextToken();
				String TIMEPOINT= tokenizer.nextToken();
				String TISSUETYPE= tokenizer.nextToken();
				String GPLID= tokenizer.nextToken();
				String ASSAYID= tokenizer.nextToken();
				String SAMPLECODE= tokenizer.nextToken();
				String REFERENCE= tokenizer.nextToken();
				String VARIANT= tokenizer.nextToken();
				String VARIANTTYPE= tokenizer.nextToken();
				String CHROMOSOME= tokenizer.nextToken(); // row key
				String POSITION= tokenizer.nextToken(); // row key
				String RSID= tokenizer.nextToken();
				String REFERENCEALLELE= tokenizer.nextToken();
				Put p = new Put(Bytes.toBytes(trial + ":" + subject + ":" + CHROMOSOME + ":" + POSITION));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("SAMPLETYPE"), Bytes.toBytes(SAMPLETYPE));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("TIMEPOINT"), Bytes.toBytes(TIMEPOINT));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("TISSUETYPE"), Bytes.toBytes(TISSUETYPE));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("GPLID"), Bytes.toBytes(GPLID));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("ASSAYID"), Bytes.toBytes(ASSAYID));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("SAMPLECODE"), Bytes.toBytes(SAMPLECODE));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("REFERENCE"), Bytes.toBytes(REFERENCE));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("VARIANTTYPE"), Bytes.toBytes(VARIANTTYPE));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("VARIANT"), Bytes.toBytes(VARIANT));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("RSID"), Bytes.toBytes(RSID));
				p.add(Bytes.toBytes(COL_FAMILY_POSITION), Bytes.toBytes("REFERENCEALLELE"), Bytes.toBytes(REFERENCEALLELE));
				count ++;
				putList.add(p);
				if (count % cachesize == 0) {
					System.out.println(count);
					subindTable.put(putList);
					putList.clear();
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			subindTable.put(putList);
			System.out.println("final count is " + count);
			long ts2 = System.currentTimeMillis();
			System.out.println("finish time is " + (ts2 - ts1));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	}
	
	public void scanBySub(String startrow, String stoprow, int threshold,
			int cacheSize) {
		System.out.println("This is the override scanBySub method ***************");
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes(COL_FAMILY_POSITION));
		s.setCacheBlocks(true);
		s.setCaching(cacheSize);
		s.setStartRow(Bytes.toBytes(startrow));
		s.setStopRow(Bytes.toBytes(stoprow));
		// s.addColumn(Bytes.toBytes(COL_FAMILY_INFO),
		// Bytes.toBytes(PATIENT_ID));
		ResultScanner scanner = null;
		long count = 0;
		try {
			scanner = subindTable.getScanner(s);
			// Scanners return Result instances.
			// Now, for the actual iteration. One way is to use a while loop
			// like so:
			long ts1 = System.currentTimeMillis();
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				if (count == threshold)
					break;
				int psnum = 0;
				for (Cell kv : rr.rawCells()) {
					psnum++;
					// each kv represents a column
					System.out.println("Row key: " + Bytes.toString(CellUtil.cloneRow(kv)));
					System.out.println("cloneFamily key: " + Bytes.toString(CellUtil.cloneFamily(kv)));
					System.out.println("cloneQualifier key: " + Bytes.toString(CellUtil.cloneQualifier(kv)));
					System.out.println("cloneValue key: " + Bytes.toString(CellUtil.cloneValue(kv)));
					System.out.println();
				}
				count++;
				if (count % 10000 == 0)
					System.out.println(count + ":" + psnum);
			}
			System.out.println("time is " + (System.currentTimeMillis() - ts1));
			System.out.println("total amount is " + count);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			scanner.close();
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args[0].equals("init")) {
			HBaseTM.init(args[1]);
		} else if (args[0].equals("insert")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			//hbasetm.insertMainSNP(args[2], args[3]);
			hbasetm.insertSubIndSNP(args[2], args[3], Integer.parseInt(args[4]));
			//hbasetm.insertPosSNP(args[2], args[3]);
		} else if (args[0].equals("scan")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.scanBySub(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
		} else {
			System.out.println("please input an argument");
			System.out.println("init for create vcf tables");
			System.out.println("scan for scan a table by subject with parameters the table name, start row name, stop row name, maximum patient number and cache size");
			System.out.println("insert for insert data into the table with parameters table name, String trial, String subject, long cachesize");
			return;
		}
	}

}

