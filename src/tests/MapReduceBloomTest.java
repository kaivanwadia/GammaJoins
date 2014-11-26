package tests;

import static org.junit.Assert.*;
import gammaSupport.ArrayConnectors;
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

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class MapReduceBloomTest {

	@Test
	public void mapReduceBloomTest1() throws Exception {
		Utility.redirectStdOut("outputFiles\\mapReduceBloomTupOut");
		mapReduceBloomTuples("tables\\client.txt", 0);
		System.out.println();
		mapReduceBloomTuples("tables\\viewing.txt", 0);
		System.out.println();
		mapReduceBloomTuples("tables\\orders.txt", 0);
		System.out.println();
		mapReduceBloomTuples("tables\\parts.txt", 0);
		System.out.println();
		mapReduceBloomTuples("tables\\odetails.txt", 0);
		Utility.validate("outputFiles\\mapReduceBloomTupOut", "correctOutput\\mapReduceBloomTupCOutput", true);
	}
	
	@Test
	public void mapReduceBloomTest2() throws Exception {
		Utility.redirectStdOut("outputFiles\\mapReduceBloomMapOut");
		mapReduceBloomMap("tables\\client.txt", 0);
		System.out.println();
		mapReduceBloomMap("tables\\viewing.txt", 0);
		System.out.println();
		mapReduceBloomMap("tables\\orders.txt", 0);
		System.out.println();
		mapReduceBloomMap("tables\\parts.txt", 0);
		System.out.println();
		mapReduceBloomMap("tables\\odetails.txt", 0);
		Utility.validate("outputFiles\\mapReduceBloomMapOut", "correctOutput\\mapReduceBloomMapCOutput", true);
	}
	
	public void mapReduceBloomMap (String fileNameA, int aJoinKey) throws Exception {
		ThreadList.init();
		System.out.println("Map Reduce Bloom Map : " + fileNameA);
		Connector readAToHsplit = new Connector("readAToHsplit");
		Connector[] hSplitToBloom = ArrayConnectors.initConnectorArray("hSplitToBloom");
		Connector[] bloomToMergeTup = ArrayConnectors.initConnectorArray("bloomToMerge");
		Connector[] bloomToMergeMap = ArrayConnectors.initConnectorArray("bloomToMergeMap");
		Connector mergeToOutTup = new Connector("mergeToOutTup");
		Connector mergeMToOutMap = new Connector("mergeMToOutMap");
		ReadRelation rRelation = new ReadRelation(fileNameA, readAToHsplit);
		HSplit hSplit = new HSplit(readAToHsplit, hSplitToBloom[0], hSplitToBloom[1], hSplitToBloom[2], hSplitToBloom[3], aJoinKey);
		Bloom bloom0 = new Bloom(hSplitToBloom[0], bloomToMergeTup[0], bloomToMergeMap[0], aJoinKey);
		Bloom bloom1 = new Bloom(hSplitToBloom[1], bloomToMergeTup[1], bloomToMergeMap[1], aJoinKey);
		Bloom bloom2 = new Bloom(hSplitToBloom[2], bloomToMergeTup[2], bloomToMergeMap[2], aJoinKey);
		Bloom bloom3 = new Bloom(hSplitToBloom[3], bloomToMergeTup[3], bloomToMergeMap[3], aJoinKey);
		
		Merge merge = new Merge(bloomToMergeTup[0], bloomToMergeTup[1], bloomToMergeTup[2], bloomToMergeTup[3], mergeToOutTup);
		MergeM mergeM = new MergeM(bloomToMergeMap[0], bloomToMergeMap[1], bloomToMergeMap[2], bloomToMergeMap[3], mergeMToOutMap);
		
		Sink sink = new Sink(mergeToOutTup);
		PrintMap printMap = new PrintMap(mergeMToOutMap);
		ThreadList.run(printMap);
	}
	
	public void mapReduceBloomTuples (String fileNameA, int aJoinKey) throws Exception {
		ThreadList.init();
		System.out.println("Map Reduce Bloom Tuples : " + fileNameA);
		Connector readAToHsplit = new Connector("readAToHsplit");
		Connector[] hSplitToBloom = ArrayConnectors.initConnectorArray("hSplitToBloom");
		Connector[] bloomToMergeTup = ArrayConnectors.initConnectorArray("bloomToMerge");
		Connector[] bloomToMergeMap = ArrayConnectors.initConnectorArray("bloomToMergeMap");
		Connector mergeToOutTup = new Connector("mergeToOutTup");
		Connector mergeMToOutMap = new Connector("mergeMToOutMap");
		ReadRelation rRelation = new ReadRelation(fileNameA, readAToHsplit);
		HSplit hSplit = new HSplit(readAToHsplit, hSplitToBloom[0], hSplitToBloom[1], hSplitToBloom[2], hSplitToBloom[3], aJoinKey);
		Bloom bloom0 = new Bloom(hSplitToBloom[0], bloomToMergeTup[0], bloomToMergeMap[0], aJoinKey);
		Bloom bloom1 = new Bloom(hSplitToBloom[1], bloomToMergeTup[1], bloomToMergeMap[1], aJoinKey);
		Bloom bloom2 = new Bloom(hSplitToBloom[2], bloomToMergeTup[2], bloomToMergeMap[2], aJoinKey);
		Bloom bloom3 = new Bloom(hSplitToBloom[3], bloomToMergeTup[3], bloomToMergeMap[3], aJoinKey);
		
		Merge merge = new Merge(bloomToMergeTup[0], bloomToMergeTup[1], bloomToMergeTup[2], bloomToMergeTup[3], mergeToOutTup);
		MergeM mergeM = new MergeM(bloomToMergeMap[0], bloomToMergeMap[1], bloomToMergeMap[2], bloomToMergeMap[3], mergeMToOutMap);
		
		SinkM sink = new SinkM(mergeMToOutMap);
		Print print = new Print(mergeToOutTup);
		ThreadList.run(print);
	}
}
