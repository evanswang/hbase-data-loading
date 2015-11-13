package dsi.kvmodel.vcf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseSNP {

	protected static final String COL_FAMILY_INFO = "info";
	protected static final String COL_FAMILY_SUBJECT = "subject";
	protected static final String COL_FAMILY_POSITION = "pos";
	
	protected static final String RS 			= "rs";
	protected static final String ALLELES 	= "alleles";
	protected static final String CHROM 		= "chrom";
	protected static final String POS 		= "pos";
	protected static final String STRAND 		= "strand";
	protected static final String ASSEMBLY 	= "assembly";
	protected static final String CENTER 		= "center";
	protected static final String PROTLSID 	= "protLSID";
	protected static final String ASSAYLSID 	= "assayLSID";
	protected static final String PANELLSID 	= "panelLSID";
	protected static final String QCCODE 		= "QCcode";

	protected Configuration config;
	protected HBaseAdmin hadmin;
	protected HTable snpTable;
	protected HTable subindTable;
	protected HTable subinfoTable;
	protected HTable posTable;

	public HBaseSNP(String tablename) throws IOException {
		config = HBaseConfiguration.create();
		snpTable = new HTable(config, tablename);
		subindTable = new HTable(config, tablename + "-subject.index");
		subinfoTable = new HTable(config, tablename + "-subject.info");
		posTable = new HTable(config, tablename + "-pos.index");
	}

	public static void init(String tablename) {
		// create the microarray table
		try {
			Configuration config = HBaseConfiguration.create();
			HBaseAdmin hadmin = new HBaseAdmin(config);
			HTableDescriptor mainTableDesc = new HTableDescriptor(tablename);
			HTableDescriptor subindTableDesc = new HTableDescriptor(tablename + "-subject.index");
			HTableDescriptor infoTableDesc = new HTableDescriptor(tablename + "-subject.info");
			HTableDescriptor posTableDesc = new HTableDescriptor(tablename + "-pos.index");
			
			HColumnDescriptor infoColDesc = new HColumnDescriptor(COL_FAMILY_INFO);			
			HColumnDescriptor subColDesc = new HColumnDescriptor(COL_FAMILY_SUBJECT);
			HColumnDescriptor posColDesc = new HColumnDescriptor(COL_FAMILY_POSITION);
			
			mainTableDesc.addFamily(infoColDesc);
			mainTableDesc.addFamily(subColDesc);	
			//hadmin.createTable(mainTableDesc);
			
			subindTableDesc.addFamily(posColDesc);
			hadmin.createTable(subindTableDesc);
			
			infoTableDesc.addFamily(infoColDesc);
			//hadmin.createTable(infoTableDesc);
			
			posTableDesc.addFamily(infoColDesc);
			posTableDesc.addFamily(subColDesc);
			//hadmin.createTable(posTableDesc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void randomread(String dataset, String filename) {
		ArrayList<Get> getList = new ArrayList<Get>();
		BufferedReader br = null;
		String str = null;
		try {
			br = new BufferedReader(new FileReader(new File(filename)));
			while ((str = br.readLine()) != null) {
				Get get = new Get(Bytes.toBytes(dataset + "+chr1+" + str));
				getList.add(get);
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
		
		try {
			int psnum = 0;
			Result[] results = snpTable.get(getList);
			for (int i = 0; i < results.length; i++)
				for (Cell kv : results[i].rawCells()) {
					psnum++;
					// each kv represents a column
					//System.out.println(Bytes.toString(kv.getRowArray()));
					//System.out.println(Bytes.toString(CellUtil
					//		.cloneQualifier(kv)));
					//System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
				}
			System.out.println("total number is " + psnum);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void randomread(String filename) {
		ArrayList<Get> getList = new ArrayList<Get>();
		BufferedReader br = null;
		String str = null;
		try {
			br = new BufferedReader(new FileReader(new File(filename)));
			while ((str = br.readLine()) != null) {
				Get get1 = new Get(Bytes.toBytes("chr1+" + str + "+asw"));
				Get get2 = new Get(Bytes.toBytes("chr1+" + str + "+ceu"));
				Get get3 = new Get(Bytes.toBytes("chr1+" + str + "+chb"));
				getList.add(get1);getList.add(get2);getList.add(get3);
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
		
		try {
			int psnum = 0;
			Result[] results = snpTable.get(getList);
			for (int i = 0; i < results.length; i++)
				for (Cell kv : results[i].rawCells()) {
					psnum++;
					// each kv represents a column
					//System.out.println(Bytes.toString(kv.getRowArray()));
					//System.out.println(Bytes.toString(CellUtil
					//		.cloneQualifier(kv)));
					//System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
				}
			System.out.println("total number is " + psnum);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	
	
	public void scanByPos(String startrow, String stoprow, int threshold,
			int cacheSize) {// only add family
		
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes(COL_FAMILY_INFO));
		s.addFamily(Bytes.toBytes(COL_FAMILY_SUBJECT));
		/*
		for (String qualifier : filterList) {
			s.addColumn(Bytes.toBytes(COL_FAMILY_RAW), Bytes.toBytes(qualifier));
		}*/
		
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
				// print out the row we found and the columns we were
				// looking for
				// System.out.println("Found row: " + rr);
				if (count == threshold)
					break;
				int psnum = 0;
				for (Cell kv : rr.rawCells()) {
					psnum++;
					// each kv represents a column
					//System.out.println(Bytes.toString(kv.getRowArray()));
					// System.out.println(Bytes.toString(CellUtil.cloneQualifier(kv)));
					// System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
				}

				count++;
				if (count % 5000 == 0)
					System.out.println(count);
				// cache 1000 	= 31.661s / 18.13s 16.052
				// cache 500	= 31.554s
				// cache 5000 	= 35.208s
			}
			System.out.println("time is " + (System.currentTimeMillis() - ts1));
			System.out.println("total amount is " + count);
			// The other approach is to use a foreach loop. Scanners are
			// iterable!
			// for (Result rr : scanner) {
			// System.out.println("Found row: " + rr);
			// }
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// Make sure you close your scanners when you are done!
			// Thats why we have it inside a try/finally clause
			scanner.close();
		}

	}

	public void scanBySub(String startrow, String stoprow, int threshold,
			int cacheSize) {// only add family
		
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes(COL_FAMILY_POSITION));
		/*
		for (String qualifier : filterList) {
			s.addColumn(Bytes.toBytes(COL_FAMILY_RAW), Bytes.toBytes(qualifier));
		}*/
		
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
				// print out the row we found and the columns we were
				// looking for
				// System.out.println("Found row: " + rr);
				if (count == threshold)
					break;
				int psnum = 0;
				for (Cell kv : rr.rawCells()) {
					psnum++;
					// each kv represents a column
					//System.out.println(Bytes.toString(kv.getRowArray()));
					// System.out.println(Bytes.toString(CellUtil.cloneQualifier(kv)));
					// System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
				}

				count++;
				System.out.println(count);
				// cache 1000 	= 31.661s / 18.13s 16.052
				// cache 500	= 31.554s
				// cache 5000 	= 35.208s
			}
			System.out.println("time is " + (System.currentTimeMillis() - ts1));
			System.out.println("total amount is " + count);
			// The other approach is to use a foreach loop. Scanners are
			// iterable!
			// for (Result rr : scanner) {
			// System.out.println("Found row: " + rr);
			// }
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// Make sure you close your scanners when you are done!
			// Thats why we have it inside a try/finally clause
			scanner.close();
		}

	}

	
	public void insertMainSNP(String trial, String filename) {

		BufferedReader br = null;
		String str = null;
		System.out.println(filename);
		long ts1 = System.currentTimeMillis();
		System.out.println("start inserting main table at " + ts1);

		int count = 0;
		try {				
		
			List<String> subList = new ArrayList<String>();
			List<Put> putList = new ArrayList<Put>();
			br = new BufferedReader(new FileReader(new File(filename)));
			while ((str = br.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(str, " ");
				String rs = tokenizer.nextToken();
				String alleles = tokenizer.nextToken();
				String chrom = tokenizer.nextToken();
				String pos = tokenizer.nextToken();
				String strand = tokenizer.nextToken();
				String assembly = tokenizer.nextToken();
				String center = tokenizer.nextToken();
				String protLSID = tokenizer.nextToken();
				String assayLSID = tokenizer.nextToken();
				String panelLSID = tokenizer.nextToken();
				String QCcode = tokenizer.nextToken();			
				
				if (rs.equals("rs#")) {					
					while (tokenizer.hasMoreTokens()) {
						String subject = tokenizer.nextToken();
						subList.add(subject);
					}
					continue;
				} else {
					Put p = new Put(Bytes.toBytes(trial + "+" + chrom + "+" + pos));	
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(RS), Bytes.toBytes(rs));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(ALLELES), Bytes.toBytes(alleles));			
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(STRAND), Bytes.toBytes(strand));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(ASSEMBLY), Bytes.toBytes(assembly));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(CENTER), Bytes.toBytes(center));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(PROTLSID), Bytes.toBytes(protLSID));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(ASSAYLSID), Bytes.toBytes(assayLSID));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(PANELLSID), Bytes.toBytes(panelLSID));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(QCCODE), Bytes.toBytes(QCcode));
									
					int i = 0;
					while (tokenizer.hasMoreTokens()) {
						p.add(Bytes.toBytes(COL_FAMILY_SUBJECT),
								Bytes.toBytes(subList.get(i++)), Bytes.toBytes(tokenizer.nextToken()));
					}
					putList.add(p);	
					count++;
					if (count % 500 == 0) {
						snpTable.put(putList);
						putList.clear();
					}
					if (count % 10000 == 0)
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

	public void insertSubInfoSNP(String trial, String filename) {
		BufferedReader br = null;
		String str = null;
		System.out.println(filename);
		long ts1 = System.currentTimeMillis();
		System.out.println("start inserting SubInfo table at " + ts1);

		int count = 0;
		try {				
		
			List<String> subList = new ArrayList<String>();
			List<Put> putList = new ArrayList<Put>();
			br = new BufferedReader(new FileReader(new File(filename)));
			while ((str = br.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(str, " ");
				String rs = tokenizer.nextToken();
				String alleles = tokenizer.nextToken();
				String chrom = tokenizer.nextToken();
				String pos = tokenizer.nextToken();
				String strand = tokenizer.nextToken();
				String assembly = tokenizer.nextToken();
				String center = tokenizer.nextToken();
				String protLSID = tokenizer.nextToken();
				String assayLSID = tokenizer.nextToken();
				String panelLSID = tokenizer.nextToken();
				String QCcode = tokenizer.nextToken();			
				
				if (rs.equals("rs#")) {					
					while (tokenizer.hasMoreTokens()) {
						String subject = tokenizer.nextToken();
						subList.add(subject);
					}
					continue;
				} else {
					Put p = new Put(Bytes.toBytes(trial + "+" + chrom + "+" + pos));	
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(RS), Bytes.toBytes(rs));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(ALLELES), Bytes.toBytes(alleles));			
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(STRAND), Bytes.toBytes(strand));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(ASSEMBLY), Bytes.toBytes(assembly));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(CENTER), Bytes.toBytes(center));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(PROTLSID), Bytes.toBytes(protLSID));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(ASSAYLSID), Bytes.toBytes(assayLSID));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(PANELLSID), Bytes.toBytes(panelLSID));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(QCCODE), Bytes.toBytes(QCcode));
									
					putList.add(p);	
					count++;
					if (count % 500 == 0) {
						snpTable.put(putList);
						putList.clear();
					}
					if (count % 10000 == 0)
						System.out.println(count);
				}
				subinfoTable.put(putList);
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

	public void insertSubIndSNP(String trial, String filename, int subjNum, int snpNum) {

		BufferedReader br = null;
		String str = null;
		System.out.println(filename);
		long ts1 = System.currentTimeMillis();
		System.out.println("start inserting SubInd table at " + ts1);

		int count = 0;
		try {				
			String [][] matrix = new String [snpNum][subjNum];
			List<String> subList = new ArrayList<String>();
			List<String> posList = new ArrayList<String>();
			br = new BufferedReader(new FileReader(new File(filename)));
			while ((str = br.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(str, " ");
				String rs = tokenizer.nextToken();
				String alleles = tokenizer.nextToken();
				String chrom = tokenizer.nextToken();
				String pos = tokenizer.nextToken();
				String strand = tokenizer.nextToken();
				String assembly = tokenizer.nextToken();
				String center = tokenizer.nextToken();
				String protLSID = tokenizer.nextToken();
				String assayLSID = tokenizer.nextToken();
				String panelLSID = tokenizer.nextToken();
				String QCcode = tokenizer.nextToken();	
				
				posList.add(pos);
				
				if (rs.equals("rs#")) {					
					while (tokenizer.hasMoreTokens()) {
						String subject = tokenizer.nextToken();
						subList.add(subject);
					}
					continue;
				} else {
														
					int i = 0;
					while (tokenizer.hasMoreTokens()) {
						matrix[count][i++] =  tokenizer.nextToken();
						//Put p = new Put(Bytes.toBytes(trial + "+" + subList.get(i++) + "+" + chrom));
						//p.add(Bytes.toBytes(COL_FAMILY_POSITION),
						//		Bytes.toBytes(pos), Bytes.toBytes(tokenizer.nextToken()));
						//subindTable.put(p);
					}
					
					count++;
					
					if (count % 10000 == 0)
						System.out.println(count + " " + System.currentTimeMillis());
				}
			}
			
			for (int i = 0; i < subjNum; i++) {
				Put p = new Put(Bytes.toBytes(trial + "+" + subList.get(i) + "+" + "chr1"));
				for (int j = 0; j < snpNum; j++) {
					p.add(Bytes.toBytes(COL_FAMILY_POSITION),
							Bytes.toBytes(posList.get(j)), Bytes.toBytes(matrix[j][i]));
				}
				subindTable.put(p);
				System.out.println(i + " " + System.currentTimeMillis());
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

	public void insertPosSNP(String trial, String filename) {

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
				StringTokenizer tokenizer = new StringTokenizer(str, " ");
				String rs = tokenizer.nextToken();
				String alleles = tokenizer.nextToken();
				String chrom = tokenizer.nextToken();
				String pos = tokenizer.nextToken();
				String strand = tokenizer.nextToken();
				String assembly = tokenizer.nextToken();
				String center = tokenizer.nextToken();
				String protLSID = tokenizer.nextToken();
				String assayLSID = tokenizer.nextToken();
				String panelLSID = tokenizer.nextToken();
				String QCcode = tokenizer.nextToken();			
				
				if (rs.equals("rs#")) {					
					while (tokenizer.hasMoreTokens()) {
						String subject = tokenizer.nextToken();
						subList.add(subject);
					}
					continue;
				} else {
					Put p = new Put(Bytes.toBytes(chrom + "+" + pos + "+" + trial));	
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(RS), Bytes.toBytes(rs));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(ALLELES), Bytes.toBytes(alleles));			
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(STRAND), Bytes.toBytes(strand));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(ASSEMBLY), Bytes.toBytes(assembly));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(CENTER), Bytes.toBytes(center));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(PROTLSID), Bytes.toBytes(protLSID));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(ASSAYLSID), Bytes.toBytes(assayLSID));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(PANELLSID), Bytes.toBytes(panelLSID));
					p.add(Bytes.toBytes(COL_FAMILY_INFO),
							Bytes.toBytes(QCCODE), Bytes.toBytes(QCcode));
									
					int i = 0;
					while (tokenizer.hasMoreTokens()) {
						p.add(Bytes.toBytes(COL_FAMILY_SUBJECT),
								Bytes.toBytes(subList.get(i++)), Bytes.toBytes(tokenizer.nextToken()));
					}
					putList.add(p);	
					count++;
					if (count % 500 == 0) {
						snpTable.put(putList);
						putList.clear();
					}
					if (count % 10000 == 0)
						System.out.println(count);
				}
				posTable.put(putList);
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
	
	private class MultipleReader implements Runnable {
		private HTable htable;
		private String filename;
		public MultipleReader (HTable htable, String filename) {
			this.htable = htable;
			this.filename = filename;
		}
		public void run() {

			ArrayList<Get> getList = new ArrayList<Get>();
			BufferedReader br = null;
			String str = null;
			try {
				br = new BufferedReader(new FileReader(new File(filename)));
				while ((str = br.readLine()) != null) {
					Get get1 = new Get(Bytes.toBytes("chr1+" + str + "+asw"));
					Get get2 = new Get(Bytes.toBytes("chr1+" + str + "+ceu"));
					Get get3 = new Get(Bytes.toBytes("chr1+" + str + "+chb"));
					getList.add(get1);getList.add(get2);getList.add(get3);
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
			
			try {
				int psnum = 0;
				Result[] results = snpTable.get(getList);
				for (int i = 0; i < results.length; i++)
					for (Cell kv : results[i].rawCells()) {
						psnum++;
						// each kv represents a column
						//System.out.println(Bytes.toString(kv.getRowArray()));
						//System.out.println(Bytes.toString(CellUtil
						//		.cloneQualifier(kv)));
						//System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
					}
				System.out.println("total number is " + psnum);
			} catch (IOException e) {
				e.printStackTrace();
			}

		
		}
	}
	
	public void scheduler (String filename, int threadNum) {
		for (int i = 0; i < threadNum; i++) {
			Runnable r = new MultipleReader(posTable, filename);
			Thread t = new Thread(r);
			t.start();
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		System.out.println("start time: " + System.currentTimeMillis());
		if (args.length < 1) {
			System.out.println("please input an argument");
			System.out
					.println("init for create a new table with a family info");
			System.out
					.println("scan for scan a table and you also need input the table name, start row name, stop row name, patient number");
			System.out.println("insert for insert data into the table, parameter table name");
			System.out.println("get for getting record");
			return;
		}
		if (args[0].equals("init")) {
			HBaseSNP.init(args[1]);
		} else if (args[0].equals("scanbypos")) {
			HBaseSNP hbasetm = new HBaseSNP(args[1]);
			hbasetm.scanByPos(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
		} else if (args[0].equals("scanbysub")) {
			HBaseSNP hbasetm = new HBaseSNP(args[1]);
			hbasetm.scanBySub(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
		} else if (args[0].equals("insert")) {
			/*
			 * parameters
			 * @tablename, which is also trial name
			 * @file path, which lists teh absolute path of all csv files
			 */
			HBaseSNP hbaseSNP = new HBaseSNP(args[1]);
			// hbaseSNP.insertMainSNP(args[2], args[3]);
			hbaseSNP.insertSubIndSNP(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
			hbaseSNP.insertSubInfoSNP(args[2], args[3]);
			hbaseSNP.insertPosSNP(args[2], args[3]);
		} else if (args[0].equals("randomread")) {
			HBaseSNP hbaseSNP = new HBaseSNP(args[1]);
			hbaseSNP.randomread(args[2], args[3]);
		} else if (args[0].equals("randomreadall")) {
			HBaseSNP hbaseSNP = new HBaseSNP(args[1]);
			hbaseSNP.randomread(args[2]);
		} else if (args[0].equals("conrandomread")) {
			HBaseSNP hbaseSNP = new HBaseSNP(args[1]);
			hbaseSNP.scheduler(args[2], Integer.parseInt(args[3]));
		} else {
		
			System.out.println("please input an argument");
			System.out
					.println("init for create a new table with a family info");
			System.out
					.println("scanBySubject for scan a table and you also need input the table name, start row name, stop row name, maximum patient number");
			System.out
			.println("scanByProbe for scan a table and you also need input the table name, start row name, stop row name, maximum probe number, cache size, patient list file");
			System.out.println("insert for insert data into the table, parameter table name");
			System.out.println("insertMatrixBySubject for insert data into the table, parameter table name, data file, annotation file, patient file, cache size");
			System.out.println("insertMatrixByProbe for insert data into the table, parameter table name, data file, annotation file, patient file, cache size");
			System.out.println("get for getting record");
			return;
		}
		System.out.println("start time: " + System.currentTimeMillis());
	}
}

