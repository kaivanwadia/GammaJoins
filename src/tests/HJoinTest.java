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
	   Utility.redirectStdOut("outputFiles/hjoinOut.txt");
	   join("tables/parts.txt", "tables/odetails.txt", 0, 1);
	   join("tables/client.txt", "tables/viewing.txt", 0, 0);
	   join("tables/orders.txt", "tables/odetails.txt", 0, 0);
	   Utility.validate("outputFiles/hjoinOut.txt", "CorrectOutput/jointest.txt",true);

	}
	
	public void join(String r1name, String r2name, int jk1, int jk2) throws Exception {
		   System.out.println( "Joining " + r1name + " with " + r2name );
		   ThreadList.init();
		   Connector c1 = new Connector("input1");
		   ReadRelation r1 = new ReadRelation(r1name, c1.getWriteEnd());
		   Connector c2 = new Connector("input2");
		   ReadRelation r2 = new ReadRelation(r2name, c2.getWriteEnd());
		   Connector o = new Connector("output");
		   HJoin hj = new HJoin(c1, c2, jk1, jk2, o);
		   Print p = new Print(o);
		   ThreadList.run(p);

		}
	
}
