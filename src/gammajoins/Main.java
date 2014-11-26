package gammajoins;

import gammaIntegration.HJoinBloomFilters;
import gammaSupport.Relation;
import gammaSupport.ThreadList;
import basicConnector.Connector;

public class Main {
	public static void main(String args[]) throws Exception {
		String fileNameA = "tables/client.txt";
		String fileNameB = "tables/viewing.txt";
		int joinKeyA = 0;
		int joinKeyB = 0;
		ThreadList.init();
		Connector readInA = new Connector("readRtoBloomA");
		Connector readInB = new Connector("readRtoBloomB");
		Connector output = new Connector("output");
		ReadRelation rRelationA = new ReadRelation(fileNameA, readInA);
		ReadRelation rRelationB = new ReadRelation(fileNameB, readInB);
		HJoinBloomFilters hJoinBF = new HJoinBloomFilters(readInA, readInB, output, joinKeyA, joinKeyB);
		
		
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
//		PrintMap printMap = new PrintMap(output);
		Print print = new Print(output);
		ThreadList.run(print);
	}
}
