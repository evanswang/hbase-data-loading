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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
public class ETLCellLine {
	protected static final String COL_FAMILY_INFO = "info";
	protected static final String COL_FAMILY_SUBJECT = "subject";
	protected static final String COL_FAMILY_POSITION = "pos";
	
	protected static final String CHROM 	= "chrom";
	protected static final String POS 		= "pos";
	protected static final String ID 		= "id";
	protected static final String REF 		= "ref";
	protected static final String ALT 		= "alt";
	protected static final String QUAL 		= "qual";
	protected static final String FILTER 	= "filter";
	protected static final String INFO 		= "info";	
	protected static final String FORMAT 	= "format";

	
	protected HTable snpTable;
	protected Configuration config;
	
	public ETLCellLine(String tablename) throws IOException {
		config = HBaseConfiguration.create();
		snpTable = new HTable(config, tablename);
	}
	
	public static void init(String tablename) {
		System.out.println("This is the override init method ***************");
		// create the microarray table
		try {
			Configuration config = HBaseConfiguration.create();
			HBaseAdmin hadmin = new HBaseAdmin(config);
			HTableDescriptor subjectTableDesc = new HTableDescriptor(tablename);
			HColumnDescriptor infoColDesc = new HColumnDescriptor(
					COL_FAMILY_INFO);
			HColumnDescriptor subColDesc = new HColumnDescriptor(
					COL_FAMILY_SUBJECT);
			HColumnDescriptor posColDesc = new HColumnDescriptor(
					COL_FAMILY_POSITION);
			if (!hadmin.tableExists(tablename)) {
				subjectTableDesc.addFamily(infoColDesc);
				subjectTableDesc.addFamily(subColDesc);
				subjectTableDesc.addFamily(posColDesc);
				hadmin.createTable(subjectTableDesc);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void insertSubIndSNP(String trial, String conceptCD, String dataFile) {
		insertSubIndSNP(trial, conceptCD, dataFile, 1000);
	}
	
	public void insertSubIndSNP(String trial, String conceptCD, String dataFile, long cachesize) {

		BufferedReader br = null;
		String str = null;
		System.out.println("Data File is " + dataFile);
		long ts1 = System.currentTimeMillis();
		System.out.println("start inserting SubInd table at " + ts1);

		int count = 0;
		List<Put> putList = new ArrayList<Put>();
		try {				
			br = new BufferedReader(new FileReader(new File(dataFile)));
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
				Put p = new Put(Bytes.toBytes(trial + ":" + conceptCD + ":" + PATIENTID + ":" + CHROMOSOME + ":" + POSITION));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("SAMPLETYPE"), Bytes.toBytes(SAMPLETYPE));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("TIMEPOINT"), Bytes.toBytes(TIMEPOINT));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("TISSUETYPE"), Bytes.toBytes(TISSUETYPE));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("GPLID"), Bytes.toBytes(GPLID));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("ASSAYID"), Bytes.toBytes(ASSAYID));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("SAMPLECODE"), Bytes.toBytes(SAMPLECODE));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("REFERENCE"), Bytes.toBytes(REFERENCE));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("VARIANTTYPE"), Bytes.toBytes(VARIANTTYPE));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("VARIANT"), Bytes.toBytes(VARIANT));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("RSID"), Bytes.toBytes(RSID));
				p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("REFERENCEALLELE"), Bytes.toBytes(REFERENCEALLELE));
				count ++;
				putList.add(p);
				if (count % cachesize == 0) {
					System.out.println(count);
					snpTable.put(putList);
					putList.clear();
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			snpTable.put(putList);
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
	
	public void insertPosSNP(String trial, String conceptCD, String filename) {
		insertPosSNP(trial, conceptCD, filename, 1000);
	}
	
	public void insertPosSNP(String trial, String conceptCD, String filename, long cachesize) {
		BufferedReader br = null;
		String str = null;
		System.out.println(filename);
		long ts1 = System.currentTimeMillis();
		System.out.println("start inserting PosSNP at " + ts1);

		int count = 0;
		try {				
		
			List<String> subList = new ArrayList<String>();
			List<Put> putList = new ArrayList<Put>();
			br = new BufferedReader(new FileReader(new File(filename)));
			while ((str = br.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(str, "\t");
				String chrom = tokenizer.nextToken();
				if (chrom.startsWith("#")) {
					if (chrom.equals("#CHROM")) {
						String pos = tokenizer.nextToken();
						String id = tokenizer.nextToken();
						String ref = tokenizer.nextToken();
						String alt = tokenizer.nextToken();
						String qual = tokenizer.nextToken();
						String filter = tokenizer.nextToken();
						String info = tokenizer.nextToken();
						String format = tokenizer.nextToken();
						while (tokenizer.hasMoreTokens()) {
							String subject = tokenizer.nextToken();
							subList.add(subject);
						}
					}
					continue;
				} else {
					String pos = tokenizer.nextToken();
					String id = tokenizer.nextToken();
					String ref = tokenizer.nextToken();
					String alt = tokenizer.nextToken();
					String qual = tokenizer.nextToken();
					String filter = tokenizer.nextToken();
					String info = tokenizer.nextToken();
					String format = tokenizer.nextToken();	
					
					Put p = new Put(Bytes.toBytes(trial + ":" + conceptCD + ":" + chrom  + ":" + pos));	
					p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes(CHROM), Bytes.toBytes(chrom));
					p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes(POS), Bytes.toBytes(pos));
					p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes(ID), Bytes.toBytes(id));
					p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes(REF), Bytes.toBytes(ref));
					p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes(ALT), Bytes.toBytes(alt));
					p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes(QUAL), Bytes.toBytes(qual));
					p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes(FILTER), Bytes.toBytes(filter));
					p.add(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes(INFO), Bytes.toBytes(info));
					p.add(Bytes.toBytes(COL_FAMILY_SUBJECT), Bytes.toBytes(FORMAT), Bytes.toBytes(format));
									
					int i = 0;
					while (tokenizer.hasMoreTokens()) {
						p.add(Bytes.toBytes(COL_FAMILY_SUBJECT),
								Bytes.toBytes(subList.get(i++)), Bytes.toBytes(tokenizer.nextToken()));
					}
					putList.add(p);
					count++;
					if (count % cachesize == 0) {
						snpTable.put(putList);
						putList.clear();
					}
					if (count % cachesize == 0)
						System.out.println(count);
				}
				snpTable.put(putList);
				putList.clear();
			}
			long ts2 = System.currentTimeMillis();
			System.out.println("finish time is " + (ts2 - ts1));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("final count is " + count);
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
			scanner = snpTable.getScanner(s);
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
			ETLCellLine.init(args[1]);
		} else if (args[0].equals("insert")) {
			ETLCellLine ETLCellLine = new ETLCellLine(args[1]);
			ETLCellLine.insertSubIndSNP(args[2], args[3], args[4], Integer.parseInt(args[5]));
		} else if (args[0].equals("scan")) {
			ETLCellLine ETLCellLine = new ETLCellLine(args[1]);
			ETLCellLine.scanBySub(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
		} else {
			System.out.println("please input an argument");
			System.out.println("init for create vcf tables");
			System.out.println("scan for scan a table by subject with parameters the table name, start row name, stop row name, maximum patient number and cache size");
			System.out.println("insert for insert data into the table with parameters table name, String trial, String subject, long cachesize");
			return;
		}
	}

}

