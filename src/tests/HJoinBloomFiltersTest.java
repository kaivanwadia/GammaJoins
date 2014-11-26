package tests;

import gammaIntegration.HJoinBloomFilters;
import gammaSupport.ThreadList;
import gammajoins.Print;
import gammajoins.ReadRelation;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class HJoinBloomFiltersTest {

	@Test
	public void HJoinBloomFiltersTest1() throws Exception {
		Utility.redirectStdOut("outputFiles/hjoinBFOut");
		HJoin("tables/client.txt", "tables/viewing.txt", 0, 0);
		System.out.println();
		HJoin("tables/orders.txt", "tables/odetails.txt", 0, 0);
		System.out.println();
		HJoin("tables/parts.txt", "tables/odetails.txt", 0, 1);
		Utility.validate("outputFiles/hjoinBFOut", "correctOutput/hjoinBFCOutput", true);
		
	}
	
	public void HJoin (String fileNameA, String fileNameB, int aJoinKey, int bJoinKey) throws Exception {
		ThreadList.init();
		Connector readInA = new Connector("readRtoBloomA");
		Connector readInB = new Connector("readRtoBloomB");
		Connector output = new Connector("output");
		ReadRelation rRelationA = new ReadRelation(fileNameA, readInA);
		ReadRelation rRelationB = new ReadRelation(fileNameB, readInB);
		HJoinBloomFilters hJoinBF = new HJoinBloomFilters(readInA, readInB, output, aJoinKey, bJoinKey);
		Print print = new Print(output);
		ThreadList.run(print);
	}
}
