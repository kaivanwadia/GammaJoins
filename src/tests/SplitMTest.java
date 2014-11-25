package tests;

import static org.junit.Assert.*;
import gammaSupport.ThreadList;
import gammajoins.Bloom;
import gammajoins.PrintMap;
import gammajoins.ReadRelation;
import gammajoins.Sink;
import gammajoins.SinkM;
import gammajoins.SplitM;

import org.junit.Test;

import basicConnector.Connector;
import RegTest.Utility;

public class SplitMTest {

	@Test
	public void splitMTest1() throws Exception {
		Utility.redirectStdOut("outputFiles/splitMOut");
		System.out.println("SPLITTING MAP : tables/client.txt on 0");
		splitMap1("tables/client.txt", 0);
		splitMap2("tables/client.txt", 0);
		splitMap3("tables/client.txt", 0);
		splitMap4("tables/client.txt", 0);
		System.out.println("--------------------------------");
		System.out.println("SPLITTING MAP : tables/viewing.txt on 0");
		splitMap1("tables/viewing.txt", 0);
		splitMap2("tables/viewing.txt", 0);
		splitMap3("tables/viewing.txt", 0);
		splitMap4("tables/viewing.txt", 0);
		System.out.println("--------------------------------");
		System.out.println("SPLITTING MAP : tables/orders.txt on 0");
		splitMap1("tables/orders.txt", 0);
		splitMap2("tables/orders.txt", 0);
		splitMap3("tables/orders.txt", 0);
		splitMap4("tables/orders.txt", 0);
		System.out.println("--------------------------------");
		System.out.println("SPLITTING MAP : tables/parts.txt on 0");
		splitMap1("tables/parts.txt", 0);
		splitMap2("tables/parts.txt", 0);
		splitMap3("tables/parts.txt", 0);
		splitMap4("tables/parts.txt", 0);
		System.out.println("--------------------------------");
		System.out.println("SPLITTING MAP : tables/odetails.txt on 1");
		splitMap1("tables/odetails.txt", 1);
		splitMap2("tables/odetails.txt", 1);
		splitMap3("tables/odetails.txt", 1);
		splitMap4("tables/odetails.txt", 1);
		Utility.validate("outputFiles/splitMOut", "correctOutput/splitMCOutput", true);
	}
	
	public void splitMap1 (String fileName, int joinKey) throws Exception {
		ThreadList.init();
		System.out.println("Split 1 : " + fileName);
		Connector readRToBloom = new Connector("readRToBloom");
		Connector bloomToSinkTuple = new Connector("bloomToSinkTuple");
		Connector bloomToSplitMap = new Connector("bloomBmapout1");
		Connector splitMToOut1 = new Connector("splitMToOut1");
		Connector splitMToOut2 = new Connector("splitMToOut2");
		Connector splitMToOut3 = new Connector("splitMToOut3");
		Connector splitMToOut4 = new Connector("splitMToOut4");
		ReadRelation rRelation = new ReadRelation(fileName, readRToBloom);
		Bloom bloom1 = new Bloom(readRToBloom, bloomToSinkTuple, bloomToSplitMap, 0);
		SplitM splitM = new SplitM(bloomToSplitMap, splitMToOut1, splitMToOut2, splitMToOut3, splitMToOut4);
		Sink sinkTuples = new Sink(bloomToSinkTuple); 
		PrintMap printMap1 = new PrintMap(splitMToOut1);
		SinkM sink2 = new SinkM(splitMToOut2);
		SinkM sink3 = new SinkM(splitMToOut3);
		SinkM sink4 = new SinkM(splitMToOut4);
		ThreadList.run(printMap1);
	}
	
	public void splitMap2 (String fileName, int joinKey) throws Exception {
		ThreadList.init();
		System.out.println("Split 2 : " + fileName);
		Connector readRToBloom = new Connector("readRToBloom");
		Connector bloomToSinkTuple = new Connector("bloomToSinkTuple");
		Connector bloomToSplitMap = new Connector("bloomBmapout1");
		Connector splitMToOut1 = new Connector("splitMToOut1");
		Connector splitMToOut2 = new Connector("splitMToOut2");
		Connector splitMToOut3 = new Connector("splitMToOut3");
		Connector splitMToOut4 = new Connector("splitMToOut4");
		ReadRelation rRelation = new ReadRelation(fileName, readRToBloom);
		Bloom bloom1 = new Bloom(readRToBloom, bloomToSinkTuple, bloomToSplitMap, 0);
		SplitM splitM = new SplitM(bloomToSplitMap, splitMToOut1, splitMToOut2, splitMToOut3, splitMToOut4);
		Sink sinkTuples = new Sink(bloomToSinkTuple); 
		PrintMap printMap2 = new PrintMap(splitMToOut2);
		SinkM sink1 = new SinkM(splitMToOut1);
		SinkM sink3 = new SinkM(splitMToOut3);
		SinkM sink4 = new SinkM(splitMToOut4);
		ThreadList.run(printMap2);
	}
	
	public void splitMap3 (String fileName, int joinKey) throws Exception {
		ThreadList.init();
		System.out.println("Split 3 : " + fileName);
		Connector readRToBloom = new Connector("readRToBloom");
		Connector bloomToSinkTuple = new Connector("bloomToSinkTuple");
		Connector bloomToSplitMap = new Connector("bloomBmapout1");
		Connector splitMToOut1 = new Connector("splitMToOut1");
		Connector splitMToOut2 = new Connector("splitMToOut2");
		Connector splitMToOut3 = new Connector("splitMToOut3");
		Connector splitMToOut4 = new Connector("splitMToOut4");
		ReadRelation rRelation = new ReadRelation(fileName, readRToBloom);
		Bloom bloom1 = new Bloom(readRToBloom, bloomToSinkTuple, bloomToSplitMap, 0);
		SplitM splitM = new SplitM(bloomToSplitMap, splitMToOut1, splitMToOut2, splitMToOut3, splitMToOut4);
		Sink sinkTuples = new Sink(bloomToSinkTuple); 
		PrintMap printMap3 = new PrintMap(splitMToOut3);
		SinkM sink1 = new SinkM(splitMToOut1);
		SinkM sink2 = new SinkM(splitMToOut2);
		SinkM sink4 = new SinkM(splitMToOut4);
		ThreadList.run(printMap3);
	}
	
	public void splitMap4 (String fileName, int joinKey) throws Exception {
		ThreadList.init();
		System.out.println("Split 4 : " + fileName);
		Connector readRToBloom = new Connector("readRToBloom");
		Connector bloomToSinkTuple = new Connector("bloomToSinkTuple");
		Connector bloomToSplitMap = new Connector("bloomBmapout1");
		Connector splitMToOut1 = new Connector("splitMToOut1");
		Connector splitMToOut2 = new Connector("splitMToOut2");
		Connector splitMToOut3 = new Connector("splitMToOut3");
		Connector splitMToOut4 = new Connector("splitMToOut4");
		ReadRelation rRelation = new ReadRelation(fileName, readRToBloom);
		Bloom bloom1 = new Bloom(readRToBloom, bloomToSinkTuple, bloomToSplitMap, 0);
		SplitM splitM = new SplitM(bloomToSplitMap, splitMToOut1, splitMToOut2, splitMToOut3, splitMToOut4);
		Sink sinkTuples = new Sink(bloomToSinkTuple); 
		PrintMap printMap4 = new PrintMap(splitMToOut4);
		SinkM sink1 = new SinkM(splitMToOut1);
		SinkM sink2 = new SinkM(splitMToOut2);
		SinkM sink3 = new SinkM(splitMToOut3);
		ThreadList.run(printMap4);
	}
}
