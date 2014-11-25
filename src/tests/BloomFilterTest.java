package tests;

import static org.junit.Assert.*;
import gammaSupport.ThreadList;
import gammajoins.Bloom;
import gammajoins.BloomFilter;
import gammajoins.Print;
import gammajoins.ReadRelation;
import gammajoins.Sink;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class BloomFilterTest {

	@Test
	public void BloomFilterTest1() throws Exception {
		Utility.redirectStdOut("outputFiles/bloomFilterOut");
		bloomFilter("tables/parts.txt", "tables/odetails.txt", 0, 1);
		System.out.println();
		bloomFilter("tables/client.txt", "tables/viewing.txt", 0, 0);
		System.out.println();
		bloomFilter("tables/orders.txt", "tables/odetails.txt", 0, 0);
		Utility.validate("outputFiles/bloomFilterOut", "correctOutput/bloomFilterCOutput", true);
	}
	
	public void bloomFilter(String table1, String table2, int jKeyT1, int jKeyT2) throws Exception {
		ThreadList.init();
		System.out.println( "Bloom Filter " + table2 + " using " + table1 );
		System.out.println();
		Connector readT1ToBloom = new Connector("readT1ToBloom");
		ReadRelation rRelation1 = new ReadRelation(table1, readT1ToBloom);
		Connector readT2ToBloomFilter = new Connector("readT2ToBloomFilter");
		ReadRelation rRelation2 = new ReadRelation(table2, readT2ToBloomFilter);
		Connector bloomToPrintTuple = new Connector("bloomTout1");
		Connector bloomToPrintMap = new Connector("bloomBmapout1");
		Bloom bloom1 = new Bloom(readT1ToBloom, bloomToPrintTuple, bloomToPrintMap, jKeyT1);
		Connector bloomFilterToPrint = new Connector("bloomFilterToPrint");
		BloomFilter bloomFilter1 = new BloomFilter(readT2ToBloomFilter, bloomToPrintMap, bloomFilterToPrint, jKeyT2);
		Sink sinkT1Tuple = new Sink(bloomToPrintTuple);
		Print printTuples = new Print(bloomFilterToPrint);
		ThreadList.run(printTuples);
	}
}
