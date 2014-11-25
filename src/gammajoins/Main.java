package gammajoins;

import gammaSupport.ThreadList;
import RegTest.Utility;
import basicConnector.Connector;

public class Main {
	public static void main(String args[]) throws Exception {
		String fileName = "tables/client.txt";
		Connector c = new Connector("rr1");
		ThreadList.init();
		ReadRelation rRelation = new ReadRelation(fileName, c.getWriteEnd());
		Print print = new Print(c.getReadEnd());
		Utility.redirectStdOut("outputFiles/readRelationOut");
		ThreadList.run(print);
		Utility.validate("outputFiles/readRelationOut", "correctOutput/readRelationOutput", true);
	}
}
