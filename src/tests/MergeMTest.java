package tests;

import static org.junit.Assert.*;
import gammaSupport.ThreadList;
import gammajoins.Bloom;
import gammajoins.HSplit;
import gammajoins.Merge;
import gammajoins.MergeM;
import gammajoins.Print;
import gammajoins.PrintMap;
import gammajoins.ReadRelation;
import gammajoins.Sink;
import gammajoins.SinkM;
import gammajoins.SplitM;

import org.junit.Test;

import basicConnector.Connector;
import RegTest.Utility;

public class MergeMTest {

	@Test
	public void mergeMTest1() throws Exception {
		Utility.redirectStdOut("outputFiles/mergeMOut");
		System.out.println("MERGING MAP : tables/client.txt on 0");
		mergeM("tables/client.txt", 0);
		System.out.println("-----------------------------");
		System.out.println("MERGING MAP : tables/viewing.txt on 0");
		mergeM("tables/viewing.txt", 0);
		System.out.println("-----------------------------");
		System.out.println("MERGING MAP : tables/orders.txt on 0");
		mergeM("tables/orders.txt", 0);
		System.out.println("-----------------------------");
		System.out.println("MERGING MAP : tables/parts.txt on 0");
		mergeM("tables/parts.txt", 0);
		System.out.println("-----------------------------");
		System.out.println("MERGING MAP : tables/odetails.txt on 1");
		mergeM("tables/odetails.txt", 1);
		Utility.validate("outputFiles/mergeMOut", "correctOutput/mergeMCOutput",true);
	}
	
	public void mergeM (String fileName, int joinKey) throws Exception {
		ThreadList.init();
		System.out.println();
		Connector readRToBloom = new Connector("readRToBloom");
		Connector bloomToSinkTuple = new Connector("bloomToSinkTuple");
		Connector bloomToSplitMap = new Connector("bloomBmapout1");
		Connector splitMToMerge1 = new Connector("splitMToMerge1");
		Connector splitMToMerge2 = new Connector("splitMToMerge2");
		Connector splitMToMerge3 = new Connector("splitMToMerge3");
		Connector splitMToMerge4 = new Connector("splitMToMerge4");
		Connector mergeMToPrint = new Connector("mergeMToPrint");
		ReadRelation rRelation = new ReadRelation(fileName, readRToBloom);
		Bloom bloom1 = new Bloom(readRToBloom, bloomToSinkTuple, bloomToSplitMap, joinKey);
		SplitM splitM = new SplitM(bloomToSplitMap, splitMToMerge1, splitMToMerge2, splitMToMerge3, splitMToMerge4);
		MergeM mergeM = new MergeM(splitMToMerge1, splitMToMerge2, splitMToMerge3, splitMToMerge4, mergeMToPrint);
		Sink sinkTuples = new Sink(bloomToSinkTuple);
		PrintMap printMap = new PrintMap(mergeMToPrint);
		ThreadList.run(printMap);
	}

}
