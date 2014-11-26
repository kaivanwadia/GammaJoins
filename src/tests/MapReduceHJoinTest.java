package tests;

import gammaIntegration.MapReduceHJoin;
import gammaSupport.ThreadList;
import gammajoins.Print;
import gammajoins.ReadRelation;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class MapReduceHJoinTest {

	@Test
	public void mapReduceHJoin() throws Exception {
		Utility.redirectStdOut("outputFiles\\mapReduceHJoinOut");
		mrHJoin("tables\\client.txt", "tables\\viewing.txt", 0, 0);
		System.out.println();
		mrHJoin("tables\\orders.txt", "tables\\odetails.txt", 0, 0);
		System.out.println();
		mrHJoin("tables\\parts.txt", "tables\\odetails.txt", 0, 1);
		Utility.validate("outputFiles\\mapReduceHJoinOut", "correctOutput\\mapReduceHJoinCOutput", true);
	}
	
	public void mrHJoin (String fileNameA, String fileNameB, int aJoinKey, int bJoinKey) throws Exception {
		ThreadList.init();
		System.out.println("Map Reduce HJoin OF " + fileNameA + " and " + fileNameB);
		Connector aToMRHJoin = new Connector("aToMRHJoin");
		Connector bToMRHJoin = new Connector("bToMRHJoin");
		Connector output = new Connector("output");
		ReadRelation readA = new ReadRelation(fileNameA, aToMRHJoin);
		ReadRelation readB = new ReadRelation(fileNameB, bToMRHJoin);
		MapReduceHJoin gamma = new MapReduceHJoin (aToMRHJoin, bToMRHJoin, output, aJoinKey, bJoinKey);
		Print print = new Print(output);
		ThreadList.run(print);
	}
	
}
