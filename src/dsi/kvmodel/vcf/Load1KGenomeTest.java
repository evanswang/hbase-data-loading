///@date 19/01/2016
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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Load1KGenomeTest {
	protected static final String COL_FAMILY_ALT = "alt";
	protected static final String COL_FAMILY_FORMAT = "format";
	protected static final String COL_FAMILY_GENOTYPE = "genotype";
	protected static final String COL_FAMILY_INFO = "info";
	protected static final String COL_FAMILY_OTHER = "other";
	
	protected HTable subjectTable;
	protected HTable posTable;
	protected HTable crossTable;
	protected HTable headerTable;
	
	public Load1KGenomeTest(String tablename)  {}
	
	public static void initHeaderTable() {}
	
	public static void initDataTable(String tablename) {}

	private void loadHeader(String header, String study) throws IOException {
		
		StringTokenizer tokenizer = new StringTokenizer(header, "#=<>,");
		String htc = tokenizer.nextToken();
		htc = htc.toLowerCase();
		
		//HTableDescriptor desc = headerTable.getTableDescriptor();
		//desc.hasFamily(Bytes.toBytes(htc))
		if (htc.equals("info") || htc.equals("format") || htc.equals("alt")) {
			// data sample
			// ##INFO=<ID=LDAF,Number=1,Type=Float,Description="MLE Allele Frequency Accounting for LD">
			String firstElem = tokenizer.nextToken();
			if (firstElem.equals("ID")) {
				System.out.println("inborn family");
				String id = tokenizer.nextToken();
				System.out.println(study + ":" + id);
				//Put p = new Put(Bytes.toBytes(study + ":" + id));
				
				System.out.println(header.split("[<>]")[1]);
					//p.add(Bytes.toBytes(htc), Bytes.toBytes(tokenizer.nextToken()),
							//Bytes.toBytes(tokenizer.nextToken()));
				
				//headerTable.put(p);
			} 
		} else {
			// data sample
			// ##fileformat=VCFv4.1
			String value = tokenizer.nextToken();
			System.out.println("other family");
			System.out.println(htc + ":" + value);
			//Put p = new Put(Bytes.toBytes(study));
			//p.add(Bytes.toBytes(COL_FAMILY_OTHER), Bytes.toBytes(htc),
			//		Bytes.toBytes(value));
			//headerTable.put(p);
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
		tokenizer.nextToken();//String format = tokenizer.nextToken();

		while (tokenizer.hasMoreTokens()) {
			subList.add(tokenizer.nextToken());
		}
	}
	
	private void loadSNP2SubjectTable(String line, String study, List<String> subList, List<Put> putList) {

		// allele data
		StringTokenizer tokenizer = new StringTokenizer(line, "\t");
		String chrom = tokenizer.nextToken();
		String pos = tokenizer.nextToken();
		String rs = tokenizer.nextToken();
		String ref = tokenizer.nextToken();
		String alt = tokenizer.nextToken();
		String qual = tokenizer.nextToken();
		String filter = tokenizer.nextToken();
		String format = tokenizer.nextToken();
		System.out.println("insert SNP into subject table");
		System.out.println(study + ":" + chrom + ":" + pos);
		System.out.println(rs + ":" + ref + ":" + alt);
		
		/*Put p = new Put(Bytes.toBytes(study + ":" + chrom + ":" + pos));	
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
				Bytes.toBytes("format"), Bytes.toBytes(format));
		putList.add(p);	*/	
		int i = 0;
		while (tokenizer.hasMoreTokens()) {
			String value = tokenizer.nextToken();
			System.out.println(study + ":" + subList.get(i++));
			System.out.println(chrom + ":" + String.format("%8d", Long.parseLong(pos)) + ":" + value);
			//Put putSubject = new Put(Bytes.toBytes(study + ":" + subList.get(i++)));
			//putSubject.add(Bytes.toBytes(COL_FAMILY_GENOTYPE),
			//		Bytes.toBytes(chrom + ":" + String.format("%8d", Long.parseLong(pos))), Bytes.toBytes(value));
			//putList.add(p);
		}	
	}
	
	public void insertSubjectTable(String study, String filename) {

		BufferedReader br = null;
		GZIPInputStream gzip = null;
		String line = null;
		System.out.println(filename);
		long ts1 = System.currentTimeMillis();
		System.out.println("start inserting main table at " + ts1);
		
		int count = 0;
		try {				
		
			List<String> subList = new ArrayList<String>();
			List<Put> putList = new ArrayList<Put>();
			gzip = new GZIPInputStream(new FileInputStream(filename));
			br = new BufferedReader(new InputStreamReader(gzip));
			while ((line = br.readLine()) != null) {
				if (line.startsWith("##")) {
					loadHeader(line, study);
				} else if (line.startsWith("#")) {
					loadColName(line, subList);	
				} else {
					loadSNP2SubjectTable(line, study, subList, putList);
					count++;
					if (count % 500 == 0) {
						subjectTable.put(putList);
						putList.clear();
					}
					if (count % 10000 == 0)
						System.out.println(count);
				}
			}
			subjectTable.put(putList);
			putList.clear();
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.out.println("please input an argument");
			System.out.println("init for create new tables");
			//System.out.println("scan for scan a table and you also need input the table name, start row name, stop row name, patient number");
			System.out.println("insert for insert data into the tables, parameter table name and input data file name");
			//System.out.println("get for getting record");
			return;
		}
		
		if (args[0].equals("init")) {
			Load1KGenomeTest.initDataTable(args[1]);
			Load1KGenomeTest.initHeaderTable();
		} else if (args[0].equals("insert")) {
			Load1KGenomeTest loader = new Load1KGenomeTest(args[1]);
			loader.insertSubjectTable(args[2], args[3]);

		}
	}

}
