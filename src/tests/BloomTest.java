package tests;

import static org.junit.Assert.*;
import gammaSupport.ThreadList;
import gammajoins.Bloom;
import gammajoins.Print;
import gammajoins.ReadRelation;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;
import basicConnector.WriteEnd;

public class BloomTest {

	@Test
	public void bloomTest1() throws Exception {
		ThreadList.init();
		String fileName = "tables/client.txt";
		Connector c = new Connector("rr1");
		ReadRelation rRelation = new ReadRelation(fileName, c.getWriteEnd());
		Connector bloomTupleOut1 = new Connector("bloomTout1");
		Connector bloomBMapOut1 = new Connector("bloomBmapout1");
		Bloom bloom1 = new Bloom(c, bloomTupleOut1, bloomBMapOut1, 0);
		Print print = new Print(bloomTupleOut1);
		Print print1 = new Print(bloomBMapOut1);
		Utility.redirectStdOut("outputFiles/readRelationOut");
		ThreadList.run(print1);
		Utility.validate("outputFiles/readRelationOut", "correctOutput/readRelationOutput", true);
	}
}
