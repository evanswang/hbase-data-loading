import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTM {

//	private static final String COL_FAMILY_INFO = "info";
	
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

	static Configuration config;

	static HBaseAdmin hadmin;
	static HTable MicroarrayTable;

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
			config = HBaseConfiguration.create();
			hadmin = new HBaseAdmin(config);
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
    public void tableScan(String startrow, String stoprow, int threshold) {
    	try {

			Scan s = new Scan();
			s.addFamily(Bytes.toBytes(COL_FAMILY_ZSCORE));
			s.setCacheBlocks(true);
			s.setCaching(5);
			s.setStartRow(Bytes.toBytes(startrow));
			s.setStopRow(Bytes.toBytes(stoprow));
			// s.addColumn(Bytes.toBytes(COL_FAMILY_INFO),
			// Bytes.toBytes(PATIENT_ID));
			ResultScanner scanner = MicroarrayTable.getScanner(s);

			long count = 0;
			try {
				// Scanners return Result instances.
				// Now, for the actual iteration. One way is to use a while loop
				// like so:
				long ts1 = System.currentTimeMillis();
				for (Result rr = scanner.next(); rr != null; rr = scanner
						.next()) {
					// print out the row we found and the columns we were
					// looking for
					// System.out.println("Found row: " + rr);
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
					}

					System.out.println(Bytes.toString(rr.getRow()) + " "
							+ psnum);

					count++;
					if (count % 10 == 0)
						System.out.println(count);
				}
				System.out.println("time is "
						+ (System.currentTimeMillis() - ts1));
				System.out.println("total amount is " + count);
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

	public void insert4MatrixBySubject(String studyname, String patientname, String annofilename, String genefilename, String datafilename, long cachesize) {
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
									+ ":" + annoList.get(probeId)), Bytes.toBytes(Math.log(Double.parseDouble(raw)) + ""));
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 1) {
			System.out.println("please input an argument");
			System.out.println("init for create a new table with a family info");
			System.out.println("iinsertMatrixBySubject for insert data into the table, parameter String studyname, String patientname, String annofilename, String genefilename, String datafilename, long cachesize=10000");
			return;
		}

		if (args[0].equals("init")) {
			init(args[1]);
		} else if (args[0].equals("insertMatrixBySubject")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.insert4MatrixBySubject(args[2], args[3], args[4], args[5], args[6], Long.parseLong(args[7]));
		} else if (args[0].equals("scan")){
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.tableScan(args[2], args[3], Integer.parseInt(args[4]));
		} else {
 		       	System.out.println("please input an argument");
    			System.out.println("init for create a new table with a family info");
			System.out.println("iinsertMatrixBySubject for insert data into the table, parameter String studyname, String patientname, String annofilename, String genefilename, String datafilename, long cachesize=10000");
    			return;
		}
	}

}
