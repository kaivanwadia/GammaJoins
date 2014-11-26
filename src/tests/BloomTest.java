package tests;

import gammaSupport.ThreadList;
import gammajoins.Bloom;
import gammajoins.Print;
import gammajoins.PrintMap;
import gammajoins.ReadRelation;
import gammajoins.Sink;
import gammajoins.SinkM;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class BloomTest {

	@Test
	public void bloomTestTupleOut1() throws Exception {
		ThreadList.init();
		String fileName = "tables\\client.txt";
		Connector readRToBloom = new Connector("rr1");
		Connector bloomToPrintTuple = new Connector("bloomTout1");
		Connector bloomToPrintMap = new Connector("bloomBmapout1");
		ReadRelation rRelation = new ReadRelation(fileName, readRToBloom);
		Bloom bloom1 = new Bloom(readRToBloom, bloomToPrintTuple, bloomToPrintMap, 0);
		SinkM sinkMap = new SinkM(bloomToPrintMap);
		Print printTuples = new Print(bloomToPrintTuple);
		Utility.redirectStdOut("outputFiles\\bloomBoxTupleOut");
		ThreadList.run(printTuples);
		Utility.validate("outputFiles\\bloomBoxTupleOut", "correctOutput\\bloomBoxTupleCOutput", true);
	}
	
	@Test
	public void bloomTestMapOut1() throws Exception {
		ThreadList.init();
		String fileName = "tables\\client.txt";
		Connector readRToBloom = new Connector("rr1");
		Connector bloomToPrintTuple = new Connector("bloomTout1");
		Connector bloomToPrintMap = new Connector("bloomBmapout1");
		ReadRelation rRelation = new ReadRelation(fileName, readRToBloom);
		Bloom bloom1 = new Bloom(readRToBloom, bloomToPrintTuple, bloomToPrintMap, 0);
		Sink sinkTuples = new Sink(bloomToPrintTuple); 
		PrintMap printMap = new PrintMap(bloomToPrintMap);
		Utility.redirectStdOut("outputFiles\\bloomBoxMapOut");
		ThreadList.run(printMap);
		Utility.validate("outputFiles\\bloomBoxMapOut", "correctOutput\\bloomBoxMapCOutput", true);
	}
}
