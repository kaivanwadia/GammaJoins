package tests;

import gammaSupport.ThreadList;
import gammajoins.HJoin;
import gammajoins.Print;
import gammajoins.ReadRelation;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class HJoinTest {

	@Test
	public void jointest() throws Exception {
	   Utility.redirectStdOut("outputFiles\\hJoinOut");
	   join("tables\\parts.txt", "tables\\odetails.txt", 0, 1);
	   System.out.println();
	   join("tables\\client.txt", "tables\\viewing.txt", 0, 0);
	   System.out.println();
	   join("tables\\orders.txt", "tables\\odetails.txt", 0, 0);
	   Utility.validate("outputFiles\\hJoinOut", "correctOutput\\hJoinCOutput",true);
	}
	
	public void join(String r1name, String r2name, int jk1, int jk2) throws Exception {
		ThreadList.init();
		System.out.println( "Joining " + r1name + " with " + r2name );
		System.out.println();
		Connector readAToHJoin = new Connector("readAToHJoin");
		ReadRelation r1 = new ReadRelation(r1name, readAToHJoin);
		Connector readBToHJoin = new Connector("readBToHJoin");
		ReadRelation r2 = new ReadRelation(r2name, readBToHJoin);
		Connector hJoinToOutput = new Connector("hJoinToOutput");
		HJoin hj = new HJoin(readAToHJoin, readBToHJoin, jk1, jk2, hJoinToOutput);
		Print p = new Print(hJoinToOutput);
		ThreadList.run(p);
	}
}
