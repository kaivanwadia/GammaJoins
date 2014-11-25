package tests;

import static org.junit.Assert.*;
import gammaSupport.ThreadList;
import gammajoins.HSplit;
import gammajoins.Merge;
import gammajoins.Print;
import gammajoins.ReadRelation;
import gammajoins.Sink;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class MergeTest {

	@Test
	public void mergeTest1() throws Exception {
		Utility.redirectStdOut("outputFiles/mergeOut");
		System.out.println("MERGING : tables/client.txt");
		merge("tables/client.txt", 0);
		System.out.println("-----------------------------");
		System.out.println("MERGING : tables/viewing.txt");
		merge("tables/viewing.txt", 0);
		System.out.println("-----------------------------");
		System.out.println("MERGING : tables/orders.txt");
		merge("tables/orders.txt", 0);
		System.out.println("-----------------------------");
		System.out.println("MERGING : tables/parts.txt");
		merge("tables/parts.txt", 0);
		System.out.println("-----------------------------");
		System.out.println("MERGING : tables/odetails.txt");
		merge("tables/odetails.txt", 0);
		Utility.validate("outputFiles/mergeOut", "correctOutput/mergeCOutput",true);
	}
	
	public void merge (String fileName, int joinKey) throws Exception {
		ThreadList.init();
		System.out.println();
		Connector readRToHSplit = new Connector("readRToHSplit");
		Connector hSplitToOut1 = new Connector("hSplitToOut1");
		Connector hSplitToOut2 = new Connector("hSplitToOut2");
		Connector hSplitToOut3 = new Connector("hSplitToOut3");
		Connector hSplitToOut4 = new Connector("hSplitToOut4");
		Connector mergeToPrint = new Connector("mergeToPrint");
		ReadRelation rRelation = new ReadRelation(fileName, readRToHSplit);
		HSplit hSplit = new HSplit(readRToHSplit, hSplitToOut1, hSplitToOut2, hSplitToOut3, hSplitToOut4, joinKey);
		Merge merge1 = new Merge(hSplitToOut1, hSplitToOut2, hSplitToOut3, hSplitToOut4, mergeToPrint);
		Print printTuples = new Print(mergeToPrint);
		ThreadList.run(printTuples);
	}
}
