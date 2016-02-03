package dsi.kvmodel.microarray;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTM {

//    private static final String COL_FAMILY_INFO = "info";
	
	private static final String COL_FAMILY_RAW = "raw";
	private static final String COL_FAMILY_LOG = "log";
	private static final String COL_FAMILY_MEAN = "mean";
	private static final String COL_FAMILY_MEDIAN = "median";
	private static final String COL_FAMILY_ZSCORE = "zscore";
	
//	private static final String COL_FAMILY_PVALUE = "pval";
	private static final String PATIENT_ID = "patient_id";
	private static final String RAW_VALUE = "raw";
	private static final String LOG_VALUE = "log";
	private static final String MEAN_VALUE = "mean";
//	private static final String STDDEV_VALUE = "stddev";
	private static final String MEDIAN_VALUE = "median";
	private static final String ZSCORE = "z_score";
//	private static final String P_VALUE = "p_value";
//	private static final String GENE_SYMBOL = "gene_symbol";
//	private static final String PROBESET_ID = "probeset";

	Configuration config;
	HBaseAdmin hadmin;
	HTable MicroarrayTable;

	public HBaseTM(String table) {
		config = HBaseConfiguration.create();
		try {
			MicroarrayTable = new HTable(config, table);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void init(String tablename) {
		// create the microarray table
		try {
			Configuration config = HBaseConfiguration.create();
			HBaseAdmin hadmin = new HBaseAdmin(config);
			HTableDescriptor microarrayTableDesc = new HTableDescriptor(
					tablename);
	//		HColumnDescriptor infoColDesc = new HColumnDescriptor(COL_FAMILY_INFO);	
			HColumnDescriptor rawColDesc = new HColumnDescriptor(COL_FAMILY_RAW); 
			HColumnDescriptor logColDesc = new HColumnDescriptor(COL_FAMILY_LOG);
			HColumnDescriptor meanColDesc = new HColumnDescriptor(COL_FAMILY_MEAN);
			HColumnDescriptor medianColDesc = new HColumnDescriptor(COL_FAMILY_MEDIAN);
			HColumnDescriptor zscoreColDesc = new HColumnDescriptor(COL_FAMILY_ZSCORE);
			
	//		microarrayTableDesc.addFamily(infoColDesc);
					
			microarrayTableDesc.addFamily(rawColDesc);
			microarrayTableDesc.addFamily(logColDesc);
			microarrayTableDesc.addFamily(meanColDesc);
			microarrayTableDesc.addFamily(medianColDesc);
			microarrayTableDesc.addFamily(zscoreColDesc);
					
			hadmin.createTable(microarrayTableDesc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void tableScan(String startrow, String stoprow, int threshold) {
		try {
			Scan s = new Scan();
			s.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
			s.setCacheBlocks(true);
			s.setCaching(5);
			s.setStartRow(Bytes.toBytes(startrow));
			s.setStopRow(Bytes.toBytes(stoprow));
			// s.setFilter(new ColumnPrefixFilter(Bytes.toBytes("DDR1")));
			// s.addColumn(Bytes.toBytes(COL_FAMILY_INFO),
			// Bytes.toBytes(PATIENT_ID)); 
			ResultScanner scanner = MicroarrayTable.getScanner(s);
			long count = 0;
			try {
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
						//System.out.println(Bytes.toString(CellUtil.cloneRow(kv)));
						//System.out.println(Bytes.toString(CellUtil.cloneQualifier(kv)));
						//System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
					}
					count++;
				}
				//System.out.println("time is " + (System.currentTimeMillis() - ts1));
				//System.out.println("total amount is " + count);
				// The other approach is to use a foreach loop. Scanners are
				// iterable!
				// for (Result rr : scanner) {
				// System.out.println("Found row: " + rr);
				// }
			} finally {
				// Make sure you close your scanners when you are done!
				// Thats why we have it inside a try/finally clause
				scanner.close();
			}
		} catch (Exception ee) {

		}
	}

	public void tableScan(String startrow, String stoprow, int threshold,
			int cacheSize, String filterFile) { // query with qualifier

		List<String> filterList = new ArrayList<String>();
		BufferedReader filterRead = null;
		
		try {
			String line = null;
			filterRead = new BufferedReader(new FileReader(filterFile));
			while ((line = filterRead.readLine()) != null) {
				filterList.add(line);
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Scan s = new Scan();
		//s.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
		
		for (String qualifier : filterList) {
			s.addColumn(Bytes.toBytes(COL_FAMILY_RAW), Bytes.toBytes(qualifier));
		}
		
		s.setCacheBlocks(true);
		s.setCaching(cacheSize);
		//s.setStartRow(Bytes.toBytes(startrow));
		//s.setStopRow(Bytes.toBytes(stoprow));
		// s.addColumn(Bytes.toBytes(COL_FAMILY_INFO),
		// Bytes.toBytes(PATIENT_ID));
		ResultScanner scanner = null;

		
		long count = 0;
		try {
			scanner = MicroarrayTable.getScanner(s);
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
					// System.out.println(Bytes.toString(kv.getRowArray()));
					// System.out.println(Bytes.toString(CellUtil.cloneQualifier(kv)));
					// System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
				}

				if (psnum != 559)
					System.out.println(Bytes.toString(rr.getRow()) + " " + psnum);

				count++;
				if (count % 5000 == 0)
					System.out.println(count);
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

	
	public void tableScan(String startrow, String stoprow, int threshold,
			int cacheSize) {// only add family
		
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
		/*
		for (String qualifier : filterList) {
			s.addColumn(Bytes.toBytes(COL_FAMILY_RAW), Bytes.toBytes(qualifier));
		}*/
		
		s.setCacheBlocks(true);
		s.setCaching(cacheSize);
		//s.setStartRow(Bytes.toBytes(startrow));
		//s.setStopRow(Bytes.toBytes(stoprow));
		// s.addColumn(Bytes.toBytes(COL_FAMILY_INFO),
		// Bytes.toBytes(PATIENT_ID));
		ResultScanner scanner = null;

		
		long count = 0;
		try {
			scanner = MicroarrayTable.getScanner(s);
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
					// System.out.println(Bytes.toString(kv.getRowArray()));
					// System.out.println(Bytes.toString(CellUtil.cloneQualifier(kv)));
					// System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
				}

				if (psnum != 559)
					System.out.println(Bytes.toString(rr.getRow()) + " " + psnum);

				count++;
				if (count % 5000 == 0)
					System.out.println(count);
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

    /**
     * insert matrix from SQL dump file
     * @param trial
     * @param filename
     */
	public void insertMicroarray(String trial, String filename) {
		// timestamp, nodename, cpu_usage, node_memory_usage,
		// node_network_I/O_speed, disk_read_speed, disk_write_speed
		BufferedReader filein;
		String line;
		StringTokenizer stin; // for deep token parse
		//Map resultMap = new HashMap();
		long count = 0;

		try {
			filein = new BufferedReader(new FileReader(filename));
			System.out.println("file " + filename);
			while ((line = filein.readLine()) != null) {
				if (line.startsWith("TRIAL_NAME")) {
					continue;
				}
				stin = new StringTokenizer(line, ",");

				String trial_name = stin.nextToken().trim();
				if (!trial_name.equals(trial)) {
					System.out.println("No more data of the trial " + trial
							+ " in file " + filename + ", the next trial is "
							+ trial_name);
					break;
				}
				String gene_symbol = stin.nextToken().trim();
				String patient_id = stin.nextToken().trim();
				String probeset = stin.nextToken().trim();
				String pvalue = stin.nextToken().trim();
				String subject_id = stin.nextToken().trim();
				String raw = stin.nextToken().trim(); // *
				String log = stin.nextToken().trim(); // *
				String mean = stin.nextToken().trim();
				String stddev = stin.nextToken().trim();
				String median = stin.nextToken().trim();
				String zscore = stin.nextToken().trim(); //

				Put p = new Put(Bytes.toBytes(patient_id));
				p.add(Bytes.toBytes(COL_FAMILY_RAW),
						Bytes.toBytes(probeset), Bytes.toBytes(raw));
				p.add(Bytes.toBytes(COL_FAMILY_LOG),
						Bytes.toBytes(probeset), Bytes.toBytes(log));
				p.add(Bytes.toBytes(COL_FAMILY_ZSCORE),
						Bytes.toBytes(probeset), Bytes.toBytes(zscore));

				MicroarrayTable.put(p);

				count++;
				if (count % 5000 == 0)
					System.out.println(count);

			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(count);
	}
	
	// (START) 16-02-15 Dilshan Silva 
    // Added the following methods to map probeId and gene symbol 
    
    private Map<String, String> mapGeneSymbol(String filePath) throws IOException {
		BufferedReader geneMapRecords = null;
		String line;
		String probeId = null;
		String geneSymbol = null;
		Map<String, String> geneMap = new HashMap<String, String>();
		try {
			geneMapRecords = new BufferedReader(new FileReader(filePath));

			while ((line = geneMapRecords.readLine()) != null) {
				//String lineNoSpaces = line.trim();
				//lineNoSpaces = lineNoSpaces.replaceAll("\\s+", "");

				StringTokenizer st = new StringTokenizer(line, ";");
				probeId = st.nextElement().toString();
				if (st.hasMoreTokens()) {
					geneSymbol = st.nextElement().toString();
				} else {
					geneSymbol = "UNKNOWN";
				}
				geneMap.put(probeId, geneSymbol);
			}
		} finally {
			try {
				geneMapRecords.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return geneMap;
	}
    // (END) 16-02-15 Dilshan Silva 

    /**
     * insert matrix using gene symbol instead of probe.
     *
     * __________________________________________________
     * _____________key_____________________|
     *   row key     |____column key________|   value
     * ______________|__family_|_qualifier__|____________
     *  study:subject|   raw   | gene	    |
     *               |   log   | gene     	|
     *               | zscore  | gene	    |
     *
     * @param studyname
     * @param patientname
     * @param annofilename
     * @param genefilename gene map file
     * @param datafilename
     * @param cachesize
     */
	public void insert4MatrixBySubject(String studyname, 
			String patientname, String annofilename, 
			String genefilename, String datafilename, long cachesize) {
		BufferedReader filein = null;
		BufferedReader annoIn = null;
		BufferedReader paIn = null;
		String line;
		StringTokenizer stin; // for deep token parse
		long count = 0;
		try {
			Map<String, String> geneMap = mapGeneSymbol(genefilename);
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
			while ((line = filein.readLine()) != null) {
				stin = new StringTokenizer(line, ",");
				int probeId = 0;
				while(stin.hasMoreTokens()) {
					String raw = stin.nextToken();
					Put p = new Put(Bytes.toBytes(studyname + ":" + paList.get(patientId)));
					p.add(Bytes.toBytes(COL_FAMILY_RAW),
							Bytes.toBytes(geneMap.get(annoList.get(probeId))
									+ ":" + annoList.get(probeId)), Bytes.toBytes(raw));
					p.add(Bytes.toBytes(COL_FAMILY_LOG),
							Bytes.toBytes(geneMap.get(annoList.get(probeId))
									+ ":" + annoList.get(probeId)), Bytes.toBytes(Math.log(Double.parseDouble(raw)) + ""));
					p.add(Bytes.toBytes(COL_FAMILY_ZSCORE),
							Bytes.toBytes(geneMap.get(annoList.get(probeId))
									+ ":" + annoList.get(probeId)), Bytes.toBytes(raw));
					putList.add(p);
					probeId++;
					count++;
					if (count % cachesize == 0) {
						System.out.println(count);
						MicroarrayTable.put(putList);
						putList.clear();
					}
				}			
				patientId ++;
			}
			System.out.println("final count is " + count);
			MicroarrayTable.put(putList);
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
	
	public void insert4MatrixByProbe(String study, String dataFile, 
			String annoFile, String patientFile, long cachesize) {
		BufferedReader dataIn = null;
		BufferedReader annoIn = null;
		BufferedReader paIn = null;
		String line;
		StringTokenizer stin; // for deep token parse
		long count = 0;
		try {
			dataIn = new BufferedReader(new FileReader(dataFile));
			annoIn = new BufferedReader(new FileReader(annoFile));
			paIn = new BufferedReader(new FileReader(patientFile));
			List<String> annoList = new ArrayList<String>();
			List<String> paList = new ArrayList<String>();
			List<Put> putList = new ArrayList<Put>();
			while ((line = annoIn.readLine()) != null) {
				annoList.add(line);
			}
			while ((line = paIn.readLine()) != null) {
				paList.add(line);
			}
			System.out.println("file " + dataFile);
			int probeId = 0;
			while ((line = dataIn.readLine()) != null) {
				stin = new StringTokenizer(line, ",");
				int paId = 0;
				while(stin.hasMoreTokens()) {
					String raw = stin.nextToken();
					Put p = new Put(Bytes.toBytes(study + ":" + annoList.get(probeId)));
					p.add(Bytes.toBytes(COL_FAMILY_RAW),
							Bytes.toBytes(paList.get(paId)), Bytes.toBytes(raw));
					p.add(Bytes.toBytes(COL_FAMILY_LOG),
							Bytes.toBytes(paList.get(paId)), Bytes.toBytes(raw));
					p.add(Bytes.toBytes(COL_FAMILY_ZSCORE),
							Bytes.toBytes(paList.get(paId)), Bytes.toBytes(raw));
					putList.add(p);
					paId++;
					count++;
					if (count % cachesize == 0) {
						System.out.println(count);
						MicroarrayTable.put(putList);
						putList.clear();
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}			
				probeId ++;
			}
			System.out.println("final count is " + count);
			MicroarrayTable.put(putList);
			putList.clear();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				dataIn.close();
				annoIn.close();
				paIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void insert4MatrixCross(String study, String dataFile, 
			String annoFile, String patientFile, long cachesize) {
		BufferedReader dataIn = null;
		BufferedReader annoIn = null;
		BufferedReader paIn = null;
		String line;
		StringTokenizer stin; // for deep token parse
		long count = 0;
		try {
			dataIn = new BufferedReader(new FileReader(dataFile));
			annoIn = new BufferedReader(new FileReader(annoFile));
			paIn = new BufferedReader(new FileReader(patientFile));
			List<String> annoList = new ArrayList<String>();
			List<String> paList = new ArrayList<String>();
			List<Put> putList = new ArrayList<Put>();
			while ((line = annoIn.readLine()) != null) {
				annoList.add(line);
			}
			while ((line = paIn.readLine()) != null) {
				paList.add(line);
			}
			System.out.println("file " + dataFile);
			int probeId = 0;
			while ((line = dataIn.readLine()) != null) {
				stin = new StringTokenizer(line, ",");
				int paId = 0;
				while(stin.hasMoreTokens()) {
					String raw = stin.nextToken();
					Put p = new Put(Bytes.toBytes(annoList.get(probeId) + ":" + study));
					p.add(Bytes.toBytes(COL_FAMILY_RAW),
							Bytes.toBytes(paList.get(paId)), Bytes.toBytes(raw));
					p.add(Bytes.toBytes(COL_FAMILY_LOG),
							Bytes.toBytes(paList.get(paId)), Bytes.toBytes(raw));
					p.add(Bytes.toBytes(COL_FAMILY_ZSCORE),
							Bytes.toBytes(paList.get(paId)), Bytes.toBytes(raw));
					putList.add(p);
					paId++;
					count++;
					if (count % cachesize == 0) {
						System.out.println(count);
						MicroarrayTable.put(putList);
						putList.clear();
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}			
				probeId ++;
			}
			System.out.println("final count is " + count);
			MicroarrayTable.put(putList);
			putList.clear();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				dataIn.close();
				annoIn.close();
				paIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void randomReadProbe(String study, String probeFile, int cache) {
		ArrayList<String> probeList = new ArrayList<String>();
		BufferedReader probeIn = null;
		try {
			probeIn = new BufferedReader(new FileReader(new File(probeFile)));
			String line;
			while ((line = probeIn.readLine()) != null)
				probeList.add(line);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				probeIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		long count = 0;
		int i = 0;
		long ts = System.currentTimeMillis();
		List<Get> getList = new ArrayList<Get>();
		try {
			for (String probe : probeList) {
				Get g = new Get(Bytes.toBytes(study + ":" + probe));
				g.addFamily(Bytes.toBytes(COL_FAMILY_ZSCORE));
				//g.setFilter(new ColumnPrefixFilter("A".getBytes()));
				getList.add(g);
				count ++;
				i ++;
				if (i == cache) {
					Result [] results = MicroarrayTable.get(getList);
					for (Result result : results)
						for (Cell cell : result.rawCells()) {
						//System.out.print(Bytes.toString(CellUtil.cloneRow(cell)) + ":");
						//System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":");
						//System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + " = ");
						//System.out.print(Bytes.toString(CellUtil.cloneValue(cell)) + "\n");
						}
					i = 0;
					getList.clear();
					System.out.println("result " + count++);
				}	
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
		System.out.println("get time is " + (System.currentTimeMillis() - ts));
	
	}
	
	public void getRecord(String assays, String filename, String prefix) {
		ArrayList<String> patientList = new ArrayList<String>();
		StringTokenizer token = new StringTokenizer(assays, ",");
		while(token.hasMoreTokens()) {
			patientList.add(token.nextToken());
		}

		long count = 0;
		long ts = System.currentTimeMillis();
		//List<Get> getlist = new ArrayList<Get>();
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
			for (String str : patientList) {
				Get g = new Get(Bytes.toBytes(str));
				g.addFamily(Bytes.toBytes(COL_FAMILY_ZSCORE));
				//g.setFilter(new ColumnPrefixFilter("A".getBytes()));
				// getlist.add(g);

				MicroarrayTable.setScannerCaching(10);
				Result r = MicroarrayTable.get(g);
				for (Cell cell : r.rawCells()) {
					pw.println("\"" + prefix
							+ Bytes.toString(CellUtil.cloneRow(cell))
							+ "\"\t\""
							+ Bytes.toString(CellUtil.cloneValue(cell))
							+ "\"\t\""
							+ Bytes.toString(CellUtil.cloneQualifier(cell))
							+ "\"\t\""
							+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "\"");
					// System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
					// System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
					// System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
				}
				System.out.println("result " + count++);
				if (r.isEmpty())
					System.out.println("no result");

			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			pw.close();
		}
		System.out.println("get time is " + (System.currentTimeMillis() - ts));
	}
	
	private class MultipleReader implements Runnable {
		private HTable htable;
		private String filename;
		private String study;
		public MultipleReader (HTable htable, String study, String filename) {
			this.htable = htable;
			this.filename = filename;
			this.study = study;
		}
		public void run() {
			long ts1 = System.currentTimeMillis();
			ArrayList<Get> getList = new ArrayList<Get>();
			BufferedReader br = null;
			String str = null;
			try {
				br = new BufferedReader(new FileReader(new File(filename)));
				while ((str = br.readLine()) != null) {
					Get get = new Get(Bytes.toBytes(study + ":" + str));
					get.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
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
				Result[] results = htable.get(getList);
				for (int i = 0; i < results.length; i++)
					for (Cell kv : results[i].rawCells()) {
						psnum++;
						// each kv represents a column
						//System.out.println(Bytes.toString(CellUtil.cloneRow(kv)));
						//System.out.println(Bytes.toString(CellUtil.cloneFamily(kv)));
						//System.out.println(Bytes.toString(CellUtil.cloneQualifier(kv)));
						//System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
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
	
	public void scheduler (String study, String probeFileName, int threadNum) {
		for (int i = 0; i < threadNum; i++) {
			Runnable r = new MultipleReader(MicroarrayTable, study, probeFileName);
			Thread t = new Thread(r);
			t.start();
		}
	}

	private class MultipleReaderCross implements Runnable {
		// TODO change Get to Scan
		private HTable htable;
		private String filename;
		public MultipleReaderCross (HTable htable, String filename) {
			this.htable = htable;
			this.filename = filename;
		}
		public void run() {
			long ts1 = System.currentTimeMillis();
			ArrayList<Get> getList = new ArrayList<Get>();
			BufferedReader br = null;
			String str = null;
			try {
				br = new BufferedReader(new FileReader(new File(filename)));
				while ((str = br.readLine()) != null) {
					Get get1 = new Get(Bytes.toBytes(str + ":" + "GSE24080"));
					get1.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
					getList.add(get1);
					Get get2 = new Get(Bytes.toBytes(str + ":" + "GSE24081"));
					get2.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
					getList.add(get2);
					Get get3 = new Get(Bytes.toBytes(str + ":" + "GSE24082"));
					get3.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
					getList.add(get3);
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
				Result[] results = htable.get(getList);
				for (int i = 0; i < results.length; i++)
					for (Cell kv : results[i].rawCells()) {
						psnum++;
						// each kv represents a column
						//System.out.println(Bytes.toString(CellUtil.cloneRow(kv)));
						//System.out.println(Bytes.toString(CellUtil.cloneFamily(kv)));
						//System.out.println(Bytes.toString(CellUtil.cloneQualifier(kv)));
						//System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
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
	
	public void schedulerCross (String probeFileName, int threadNum) {
		for (int i = 0; i < threadNum; i++) {
			Runnable r = new MultipleReaderCross(MicroarrayTable, probeFileName);
			Thread t = new Thread(r);
			t.start();
		}
	}
	
	private class MultipleScanCross implements Runnable {
		private HTable htable;
		private String filename;
		public MultipleScanCross (HTable htable, String filename) {
			this.htable = htable;
			this.filename = filename;
		}
		public void run() {
			long ts1 = System.currentTimeMillis();
			BufferedReader br = null;
			String str = null;
			try {
				br = new BufferedReader(new FileReader(new File(filename)));
				while ((str = br.readLine()) != null) {
					// scan
					tableScan(str + ":", str + ";", 999);
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
			long ts2 = System.currentTimeMillis();
			System.out.println("start time is " + ts1 + ", end time is " + ts2);
		}
	}
	
	public void schedulerScanCross (String probeFileName, int threadNum) {
		for (int i = 0; i < threadNum; i++) {
			Runnable r = new MultipleScanCross(MicroarrayTable, probeFileName);
			Thread t = new Thread(r);
			t.start();
		}
	}
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 1) {
			System.out.println("please input an argument");
			System.out.println("init for create a new table with a family info");
			System.out.println("scan for scan a table and you also need input the table name, start row name, stop row name, patient number");
			System.out.println("insert for insert data into the table, parameter table name");
			System.out.println("get for getting record");
			return;
		}
		if (args[0].equals("init")) {
			HBaseTM.init(args[1]);
		} else if (args[0].equals("scanBySubject")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.tableScan(args[2], args[3], Integer.parseInt(args[4]));
		} else if (args[0].equals("scanByProbe")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.tableScan(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]), args[6]);
		} else if (args[0].equals("insert")) {
			/*
			 * parameters
			 * @tablename, which is also trial name
			 * @file path, which lists teh absolute path of all csv files
			 */
			HBaseTM hbasetm = new HBaseTM(args[1]);
			BufferedReader filein = null;
			String line;
			try {
				filein = new BufferedReader(new FileReader(args[2]));
				while ((line = filein.readLine()) != null) {
					hbasetm.insertMicroarray(args[1], line);
				}
			} catch (FileNotFoundException e) {
				System.out.println("cannot find the file.");
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					filein.close();
				} catch (IOException e) {
					System.out.println("cannot close the file.");
					e.printStackTrace();
				}
			}
			//hbasetm.insertMicroarray("GSE4382.csv");
			/*
			for (int i = 0; i < 5; i++)
				for (int j = 0; j < 26; j++)
					hbasetm.insertMicroarray("x" + (char) ('a' + i)
							+ (char) ('a' + j));
							*/
			// System.out.println("x" + (char)('a' + i) + (char)('a' + j));

		} else if (args[0].equals("get")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.getRecord(args[2], args[3], args[4]);
		} else if (args[0].equals("randomReadProbe")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.randomReadProbe(args[2], args[3], Integer.parseInt(args[4]));
		} else if (args[0].equals("conrandomread")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.scheduler(args[2], args[3], Integer.parseInt(args[4]));
		} else if (args[0].equals("concross")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.schedulerCross(args[2], Integer.parseInt(args[3]));
		} else if (args[0].equals("concrossscan")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.schedulerScanCross(args[2], Integer.parseInt(args[3]));
		} else if (args[0].equals("insertMatrixBySubject")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.insert4MatrixBySubject(args[2], args[3], args[4], args[5], args[6], Long.parseLong(args[7]));
		} else if (args[0].equals("insertMatrixByProbe")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.insert4MatrixByProbe(args[2], args[3], args[4], args[5], Long.parseLong(args[6]));
		} else if (args[0].equals("insertMatrixCross")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.insert4MatrixCross(args[2], args[3], args[4], args[5], Long.parseLong(args[6]));
		} else {
			System.out.println("please input an argument");
			System.out.println("init for create a new table with a family info");
			System.out.println("scanBySubject for scan a table and you also need input the table name, start row name, stop row name, maximum patient number");
			System.out.println("scanByProbe for scan a table and you also need input the table name, start row name, stop row name, maximum probe number, cache size, patient list file");
			System.out.println("insert for insert data into the table, parameter table name");
			System.out.println("insertMatrixBySubject for insert data into the table, parameter table name, data file, annotation file, patient file, cache size");
			System.out.println("insertMatrixByProbe for insert data into the table, parameter table name, data file, annotation file, patient file, cache size");
			System.out.println("get for getting record");
			return;
		}		
	}

}

