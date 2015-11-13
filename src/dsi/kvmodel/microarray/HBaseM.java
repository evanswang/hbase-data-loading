package dsi.kvmodel.microarray;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseM {
	protected static final String COL_FAMILY_RAW = "raw";
	protected static final String COL_FAMILY_LOG = "log";
	protected static final String COL_FAMILY_ZSCORE = "zscore";
	protected static final String RAW_VALUE = "raw";
	protected static final String LOG_VALUE = "log";
	protected static final String ZSCORE_VALUE = "zscore";
	static Configuration config;
	static HBaseAdmin hadmin;
	static HTable MicroarrayTable;
	private static Map<String, String> geneMap = new HashMap<String, String>();

	public HBaseM(String table) {

		config = HBaseConfiguration.create();
		try {
			MicroarrayTable = new HTable(config, table);
		} catch (IOException e) {
			init("microarray");
			try {
				MicroarrayTable = new HTable(config, table);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			//System.err.println("Table not exist, please initialize the table");
		}
	}

	public static void init(String tablename) {
		// create the microarray table
		try {
			config = HBaseConfiguration.create();
			hadmin = new HBaseAdmin(config);
			HTableDescriptor microarrayTableDesc = new HTableDescriptor(
					tablename);
			HColumnDescriptor rawColDesc = new HColumnDescriptor(COL_FAMILY_RAW);
			HColumnDescriptor logColDesc = new HColumnDescriptor(COL_FAMILY_LOG);
			HColumnDescriptor zscoreColDesc = new HColumnDescriptor(COL_FAMILY_ZSCORE);
			microarrayTableDesc.addFamily(rawColDesc);
			microarrayTableDesc.addFamily(logColDesc);
			microarrayTableDesc.addFamily(zscoreColDesc);
			hadmin.createTable(microarrayTableDesc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// (START) 16-02-15 Dilshan Silva
	// Added the following methods to map probeId and gene symbol
	public void skipLines(BufferedReader geneMapRecords, int numberOfLinesToSkip) {
		try {
			for (int i = 0; i < numberOfLinesToSkip; i++) {
				geneMapRecords.readLine();
			}
		} catch (Exception e) {
		}
	}

	public void mapGeneSymbol(String filePath) {
		BufferedReader geneMapRecords = null;
		String line;
		String probeId = null;
		String geneSymbol = null;

		try {
			geneMapRecords = new BufferedReader(new FileReader(filePath));
			// Skip the first 2 lines
			skipLines(geneMapRecords, 2);

			while ((line = geneMapRecords.readLine()) != null) {
				String lineNoSpaces = line.trim();
				lineNoSpaces = lineNoSpaces.replaceAll("\\s+", "");

				StringTokenizer st = new StringTokenizer(lineNoSpaces, "|");
				probeId = st.nextElement().toString();
				if (st.hasMoreTokens()) {
					geneSymbol = st.nextElement().toString();
				} else {
					geneSymbol = "UNKNOWN";
				}
				geneMap.put(probeId, geneSymbol);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				geneMapRecords.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// (END) 16-02-15 Dilshan Silva

	// Study Id GSE1456
	public void insertMatrixBySubject(String filename, String annofilename,
			String patientname, long cachesize) {
		BufferedReader filein = null;
		BufferedReader annoIn = null;
		BufferedReader paIn = null;
		String line;
		StringTokenizer stin; // for deep token parse
		long count = 0;
		try {
			filein = new BufferedReader(new FileReader(filename));
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
			System.out.println("file " + filename);
			int patientId = 0;
			while ((line = filein.readLine()) != null) {
				stin = new StringTokenizer(line, ",");
				int probeId = 0;
				while (stin.hasMoreTokens()) {
					String raw = stin.nextToken();

					Put p = new Put(Bytes.toBytes(paList.get(patientId)));
					p.add(Bytes.toBytes(COL_FAMILY_RAW),
							Bytes.toBytes(geneMap.get(annoList.get(probeId))
									+ ":" + annoList.get(probeId)),
							Bytes.toBytes(raw));
					putList.add(p);
					probeId++;
					count++;
					if (count % cachesize == 0) {
						System.out.println(count);
						MicroarrayTable.put(putList);
						putList.clear();

					}
				}
				patientId++;

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

	public void tableScan(String startrow, String stoprow, int threshold) {
		try {

			Scan s = new Scan();
			s.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
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
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		HBaseM hbasetm = new HBaseM("Microarray_Subject");
		// Change directory path to location of files
		String directoryLocation = "C:\\Dilshan\\Projects\\HBaseDataLoader\\data\\";

		String geneMapFile = "gene2probe.txt";
		hbasetm.mapGeneSymbol(directoryLocation + geneMapFile);

		String dataFile = "gse1456.data.row";
		String annotationFile = "gse1456.probenames";
		String patientNumberFile = "gse1456.patientnum";

		hbasetm.insertMatrixBySubject(directoryLocation + dataFile,
				directoryLocation + annotationFile, directoryLocation
						+ patientNumberFile, 0);

	}

}
