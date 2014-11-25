package tests;

import static org.junit.Assert.*;
import gammaSupport.ThreadList;
import gammajoins.Print;
import gammajoins.ReadRelation;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class ReadRelationTest {

	@Test
	public void readRelationTest1 () throws Exception {
		ThreadList.init();
		String fileName = "tables/client.txt";
		Connector c = new Connector("rr1");
		ReadRelation rRelation = new ReadRelation(fileName, c.getWriteEnd());
		Print print = new Print(c);
		Utility.redirectStdOut("outputFiles/readRelationOut");
		ThreadList.run(print);
		Utility.validate("outputFiles/readRelationOut", "correctOutput/readRelationOutput", true);
	}
}