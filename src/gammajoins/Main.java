package gammajoins;

import gammaSupport.ThreadList;
import RegTest.Utility;
import basicConnector.Connector;

public class Main {
	public static void main(String args[]) throws Exception {
		
		   ThreadList.init();
		   String r1name = "tables/parts.txt";
		   String r2name = "tables/odetails.txt";
		   System.out.println( "Joining " + r1name + " with " + r2name );
		   int jk1 = 0;
		   int jk2 = 1;
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
