///@date 19/01/2016
///@author wsc
/// loading 1000 Genome VCF data

package dsi.kvmodel.vcf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Load1KGenome {
	protected static final String COL_FAMILY_ALT = "alt";
	protected static final String COL_FAMILY_FORMAT = "format";
	protected static final String COL_FAMILY_GENOTYPE = "genotype";
	protected static final String COL_FAMILY_INFO = "info";
	protected static final String COL_FAMILY_OTHER = "other";
	
	protected HTable subjectTable;
	protected HTable posTable;
	protected HTable crossTable;
	protected HTable headerTable;
	
	public Load1KGenome(String tablename) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin hadmin = new HBaseAdmin(config);
		if (hadmin.tableExists(tablename + "-subject")
				&& hadmin.tableExists(tablename + "-position")
				&& hadmin.tableExists(tablename + "-cross")) {
			subjectTable = new HTable(config, tablename + "-subject");
			posTable = new HTable(config, tablename + "-position");
			crossTable = new HTable(config, tablename + "-cross");
		} else {
			throw new IOException("data tables not exist!!!");
		}
		
		if (hadmin.tableExists("header")) {
			headerTable = new HTable(config, "header");
		} else {
			throw new IOException("header table not exist!!!");
		}
	}
	
	public static void initHeaderTable() {
		// create the microarray table
		try {
			Configuration config = HBaseConfiguration.create();
			HBaseAdmin hadmin = new HBaseAdmin(config);
			HTableDescriptor headerTableDesc = new HTableDescriptor("header");
			
			HColumnDescriptor infoColDesc = new HColumnDescriptor(COL_FAMILY_INFO);			
			HColumnDescriptor altColDesc = new HColumnDescriptor(COL_FAMILY_ALT);
			HColumnDescriptor formatColDesc = new HColumnDescriptor(COL_FAMILY_FORMAT);
			HColumnDescriptor otherColDesc = new HColumnDescriptor(COL_FAMILY_OTHER);
			
			headerTableDesc.addFamily(infoColDesc);
			headerTableDesc.addFamily(altColDesc);	
			headerTableDesc.addFamily(formatColDesc);
			headerTableDesc.addFamily(otherColDesc);
			
			hadmin.createTable(headerTableDesc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void initDataTable(String tablename) {
		// create the microarray table
		try {
			Configuration config = HBaseConfiguration.create();
			HBaseAdmin hadmin = new HBaseAdmin(config);
			HTableDescriptor subjectTableDesc = new HTableDescriptor(tablename + "-subject");
			HTableDescriptor posTableDesc = new HTableDescriptor(tablename + "-position");
			HTableDescriptor crossTableDesc = new HTableDescriptor(tablename + "-cross");
			
			HColumnDescriptor infoColDesc = new HColumnDescriptor(COL_FAMILY_INFO);			
			HColumnDescriptor genotypeColDesc = new HColumnDescriptor(COL_FAMILY_GENOTYPE);
			
			subjectTableDesc.addFamily(infoColDesc);
			subjectTableDesc.addFamily(genotypeColDesc);	
			hadmin.createTable(subjectTableDesc);
			
			posTableDesc.addFamily(infoColDesc);
			posTableDesc.addFamily(genotypeColDesc);	
			hadmin.createTable(posTableDesc);
			
			crossTableDesc.addFamily(infoColDesc);
			crossTableDesc.addFamily(genotypeColDesc);	
			hadmin.createTable(crossTableDesc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void loadHeader(String header, String study) throws IOException {
		
		StringTokenizer tokenizer = new StringTokenizer(header, "#=<>,");
		String htc = tokenizer.nextToken();
		htc = htc.toLowerCase();
		
		HTableDescriptor desc = headerTable.getTableDescriptor();
		if (desc.hasFamily(Bytes.toBytes(htc))) {
			// data sample
			// ##INFO=<ID=LDAF,Number=1,Type=Float,Description="MLE Allele Frequency Accounting for LD">
			String firstElem = tokenizer.nextToken();
			if (firstElem.equals("ID")) {
				String id = tokenizer.nextToken();
				Put p = new Put(Bytes.toBytes(study));
				p.add(Bytes.toBytes(htc), Bytes.toBytes(id),
						Bytes.toBytes(header.split("[<>]")[1]));

				headerTable.put(p);
			} 
		} else {
			// data sample
			// ##fileformat=VCFv4.1
			String value = tokenizer.nextToken();
			Put p = new Put(Bytes.toBytes(study));
			p.add(Bytes.toBytes(COL_FAMILY_OTHER), Bytes.toBytes(htc),
					Bytes.toBytes(value));
			headerTable.put(p);
		}
	}
	
	private void loadColName(String line, List<String> subList) {
		StringTokenizer tokenizer = new StringTokenizer(line, "\t");
		tokenizer.nextToken();//String chrom = tokenizer.nextToken();
		tokenizer.nextToken();//String pos = tokenizer.nextToken();
		tokenizer.nextToken();//String rs = tokenizer.nextToken();
		tokenizer.nextToken();//String ref = tokenizer.nextToken();
		tokenizer.nextToken();//String alt = tokenizer.nextToken();
		tokenizer.nextToken();//String qual = tokenizer.nextToken();
		tokenizer.nextToken();//String filter = tokenizer.nextToken();
		tokenizer.nextToken();//String info = tokenizer.nextToken();
		tokenizer.nextToken();//String format = tokenizer.nextToken();
		while (tokenizer.hasMoreTokens()) {
			subList.add(tokenizer.nextToken());
		}
	}
	
	private void addSNP2SubjectPutList(String line, String study, List<String> subList, List<Put> putList) {

		// allele data
		StringTokenizer tokenizer = new StringTokenizer(line, "\t");
		String chrom = tokenizer.nextToken();
		String pos = tokenizer.nextToken();
		String rs = tokenizer.nextToken();
		String ref = tokenizer.nextToken();
		String alt = tokenizer.nextToken();
		String qual = tokenizer.nextToken();
		String filter = tokenizer.nextToken();
		String info = tokenizer.nextToken();
		String format = tokenizer.nextToken();
		
		Put p = new Put(Bytes.toBytes(study + ":" + chrom + ":" + String.format("%8d", Long.parseLong(pos))));	
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("rs"), Bytes.toBytes(rs));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("ref"), Bytes.toBytes(ref));			
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("alt"), Bytes.toBytes(alt));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("qual"), Bytes.toBytes(qual));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("filter"), Bytes.toBytes(filter));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("info"), Bytes.toBytes(info));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("format"), Bytes.toBytes(format));
		putList.add(p);		
		int i = 0;
		while (tokenizer.hasMoreTokens()) {
			String value = tokenizer.nextToken();
			Put putSubject = new Put(Bytes.toBytes(study + ":" + subList.get(i++)));
			putSubject.add(Bytes.toBytes(COL_FAMILY_GENOTYPE),
					Bytes.toBytes(chrom + ":" + String.format("%8d", Long.parseLong(pos))), Bytes.toBytes(value));
			putList.add(putSubject);
			
		}	
	}
	
	private void addSNP2PositionPutList(String line, String study, List<String> subList, List<Put> putList) {

		// allele data
		StringTokenizer tokenizer = new StringTokenizer(line, "\t");
		String chrom = tokenizer.nextToken();
		String pos = tokenizer.nextToken();
		String rs = tokenizer.nextToken();
		String ref = tokenizer.nextToken();
		String alt = tokenizer.nextToken();
		String qual = tokenizer.nextToken();
		String filter = tokenizer.nextToken();
		String info = tokenizer.nextToken();
		String format = tokenizer.nextToken();
		
		Put p = new Put(Bytes.toBytes(study + ":" + chrom + ":" + String.format("%8d", Long.parseLong(pos))));	
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("rs"), Bytes.toBytes(rs));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("ref"), Bytes.toBytes(ref));			
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("alt"), Bytes.toBytes(alt));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("qual"), Bytes.toBytes(qual));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("filter"), Bytes.toBytes(filter));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("info"), Bytes.toBytes(info));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("format"), Bytes.toBytes(format));
		putList.add(p);		
		int i = 0;
		while (tokenizer.hasMoreTokens()) {
			String value = tokenizer.nextToken();
			Put putSubject = new Put(Bytes.toBytes(study + ":" + chrom + ":" + String.format("%8d", Long.parseLong(pos))));
			putSubject.add(Bytes.toBytes(COL_FAMILY_GENOTYPE),
					Bytes.toBytes(subList.get(i++)), Bytes.toBytes(value));
			putList.add(putSubject);
		}	
	}
	
	private void addSNP2CrossPutList(String line, String study, List<String> subList, List<Put> putList) {

		// allele data
		StringTokenizer tokenizer = new StringTokenizer(line, "\t");
		String chrom = tokenizer.nextToken();
		String pos = tokenizer.nextToken();
		String rs = tokenizer.nextToken();
		String ref = tokenizer.nextToken();
		String alt = tokenizer.nextToken();
		String qual = tokenizer.nextToken();
		String filter = tokenizer.nextToken();
		String info = tokenizer.nextToken();
		String format = tokenizer.nextToken();
		
		Put p = new Put(Bytes.toBytes(chrom + ":" + String.format("%8d", Long.parseLong(pos))  + ":" + study));	
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("rs"), Bytes.toBytes(rs));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("ref"), Bytes.toBytes(ref));			
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("alt"), Bytes.toBytes(alt));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("qual"), Bytes.toBytes(qual));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("filter"), Bytes.toBytes(filter));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("info"), Bytes.toBytes(info));
		p.add(Bytes.toBytes(COL_FAMILY_INFO),
				Bytes.toBytes("format"), Bytes.toBytes(format));
		putList.add(p);		
		int i = 0;
		while (tokenizer.hasMoreTokens()) {
			String value = tokenizer.nextToken();
			Put putSubject = new Put(Bytes.toBytes(chrom + ":" + String.format("%8d", Long.parseLong(pos))  + ":" + study));
			putSubject.add(Bytes.toBytes(COL_FAMILY_GENOTYPE),
					Bytes.toBytes(subList.get(i++)), Bytes.toBytes(value));
			putList.add(putSubject);
		}	
	}
	
	public void insert(String study, String filename) {
		insert(study, filename, 500);
	}
	
	public void insert(String study, String filename, int cachesize) {
		BufferedReader br = null;
		GZIPInputStream gzip = null;
		String line = null;
		System.out.println(filename);
		long ts1 = System.currentTimeMillis();
		System.out.println("start inserting main table at " + ts1);
		int count = 0;
		try {				
			List<String> subList = new ArrayList<String>();
			List<Put> subjectPutList = new ArrayList<Put>();
			List<Put> posPutList = new ArrayList<Put>();
			List<Put> crossPutList = new ArrayList<Put>();
			gzip = new GZIPInputStream(new FileInputStream(filename));
			br = new BufferedReader(new InputStreamReader(gzip));
			while ((line = br.readLine()) != null) {
				if (line.startsWith("##")) {
					loadHeader(line, study);
				} else if (line.startsWith("#")) {
					loadColName(line, subList);	
				} else {
					addSNP2SubjectPutList(line, study, subList, subjectPutList);
					addSNP2PositionPutList(line, study, subList, posPutList);
					addSNP2CrossPutList(line, study, subList, crossPutList);
					count++;
					if (count % cachesize == 0) {
						subjectTable.put(subjectPutList);
						subjectPutList.clear();
						posTable.put(posPutList);
						posPutList.clear();
						crossTable.put(crossPutList);
						crossPutList.clear();
					}
					if (count % 10000 == 0)
						System.out.println(count);
				}
				subjectTable.put(subjectPutList);
				subjectPutList.clear();
				posTable.put(posPutList);
				posPutList.clear();
				crossTable.put(crossPutList);
				crossPutList.clear();
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
				gzip.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void scan(String startrow, String stoprow, int threshold,
			int cacheSize) {// only add family
		// 1kgenome:19:   90974
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes(COL_FAMILY_GENOTYPE));
		//s.addFamily(Bytes.toBytes(COL_FAMILY_SUBJECT));
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
			scanner = posTable.getScanner(s);
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
				//System.out.println(psnum);
				count++;
				if (count % cacheSize == 0) {
					System.out.println(count);
					System.out.println(System.currentTimeMillis());
				}
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

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("please input an argument");
			System.out.println("init for create new tables");
			System.out.println("scan for scan a table and you also need input the table name, start row name, stop row name, patient number, cache size");
			System.out.println("insert for insert data into the tables, parameter table name and input data file name");
			//System.out.println("get for getting record");
			return;
		}
		
		if (args[0].equals("init")) {
			Load1KGenome.initDataTable(args[1]);
			Load1KGenome.initHeaderTable();
		} else if (args[0].equals("insert")) {
			try {
				Load1KGenome loader = new Load1KGenome(args[1]);
				loader.insert(args[2], args[3]);
			} catch (MasterNotRunningException e) {
				e.printStackTrace();
			} catch (ZooKeeperConnectionException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else if (args[0].equals("scan")) {
			try {
				Load1KGenome loader = new Load1KGenome(args[1]);
				loader.scan(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
			} catch (MasterNotRunningException e) {
				e.printStackTrace();
			} catch (ZooKeeperConnectionException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
