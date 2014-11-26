package gammajoins;

import gammaIntegration.MapReduceBloom;
import gammaSupport.ThreadList;
import basicConnector.Connector;

public class Main {
	public static void main(String args[]) throws Exception {
		String fileName = "tables/client.txt";
		int joinKey = 0;
		ThreadList.init();
		Connector readIn = new Connector("readRtoBloom");
		Connector mergeOut = new Connector("bloomMergeOut");
		Connector mmergeOut = new Connector("bloomMMergeOut");
		ReadRelation rRelation = new ReadRelation(fileName, readIn);
		MapReduceBloom hjf = new MapReduceBloom(readIn, joinKey, mergeOut, mmergeOut); 
		
//		Connector readRToBloom = new Connector("readRToBloom");
//		Connector bloomToSinkTuple = new Connector("bloomToSinkTuple");
//		Connector bloomToSplitMap = new Connector("bloomBmapout1");
//		Connector splitMToMerge1 = new Connector("splitMToMerge1");
//		Connector splitMToMerge2 = new Connector("splitMToMerge2");
//		Connector splitMToMerge3 = new Connector("splitMToMerge3");
//		Connector splitMToMerge4 = new Connector("splitMToMerge4");
//		Connector mergeMToPrint = new Connector("mergeMToPrint");
//		ReadRelation rRelation = new ReadRelation(fileName, readRToBloom);
//		Bloom bloom1 = new Bloom(readRToBloom, bloomToSinkTuple, bloomToSplitMap, 0);
//		SplitM splitM = new SplitM(bloomToSplitMap, splitMToMerge1, splitMToMerge2, splitMToMerge3, splitMToMerge4);
//		MergeM mergeM = new MergeM(splitMToMerge1, splitMToMerge2, splitMToMerge3, splitMToMerge4, mergeMToPrint);
//		Sink sinkTuples = new Sink(bloomToSinkTuple);
		PrintMap printMap = new PrintMap(mmergeOut);
		Print print = new Print(mergeOut);
		ThreadList.run(print);
	}
}
