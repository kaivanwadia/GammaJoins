package tests;

import gammaIntegration.MapReduceBFilter;
import gammaSupport.ThreadList;
import gammajoins.Bloom;
import gammajoins.Print;
import gammajoins.ReadRelation;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class MapReduceBFilterTest {
	@Test
	public void MapReduceBFilterTest() throws Exception {
		Utility.redirectStdOut("outputFiles\\mapReduceBFOut");
		mapReduceBF("tables\\client.txt", "tables\\viewing.txt", 0, 0);
		System.out.println();
		mapReduceBF("tables\\orders.txt", "tables\\odetails.txt", 0, 0);
		System.out.println();
		mapReduceBF("tables\\parts.txt", "tables\\odetails.txt", 0, 1);
		Utility.validate("outputFiles\\mapReduceBFOut", "correctOutput\\mapReduceBFCOutput", true);
		
	}
	
	public void mapReduceBF (String fileNameA, String fileNameB, int aJoinKey, int bJoinKey) throws Exception {
		ThreadList.init();
		Connector readInA = new Connector("readMapreduceBFilterA");
		Connector readInB = new Connector("readMapreduceBFilterB");
		Connector output = new Connector("output");
		ReadRelation rRelationA = new ReadRelation(fileNameA, readInA);
		ReadRelation rRelationB = new ReadRelation(fileNameB, readInB);
		Connector bloomOut = new Connector("mergeMapA");
		Connector bloomAMap = new Connector("mmergeMapA");
		Bloom bloomA = new Bloom(readInA, bloomOut, bloomAMap, aJoinKey);
		MapReduceBFilter mapReduceBFilter = new MapReduceBFilter(bloomAMap, readInB, bJoinKey, output);
		Print print = new Print(output);
		ThreadList.run(print);
	}
}
