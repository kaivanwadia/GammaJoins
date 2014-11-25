package gammajoins;

import gammaSupport.ThreadList;
import RegTest.Utility;
import basicConnector.Connector;

public class Main {
	public static void main(String args[]) throws Exception {
		String fileName = "tables/client.txt";
		int joinKey = 0;
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
		PrintMap printMap2 = new PrintMap(splitMToOut2);
		SinkM sink1 = new SinkM(splitMToOut1);
//		SinkM sink2 = new SinkM(splitMToOut2);
		SinkM sink3 = new SinkM(splitMToOut3);
		SinkM sink4 = new SinkM(splitMToOut4);
		ThreadList.run(printMap2);
	}
}
