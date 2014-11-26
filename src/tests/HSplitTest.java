package tests;

import gammaSupport.ThreadList;
import gammajoins.HSplit;
import gammajoins.Print;
import gammajoins.ReadRelation;
import gammajoins.Sink;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class HSplitTest {
	
	@Test
	public void hSplitTest1() throws Exception {
		Utility.redirectStdOut("outputFiles\\hSplitOut");
		System.out.println("SPLITTING : tables\\client.txt");
		hSplit1("tables\\client.txt", 0);
		hSplit2("tables\\client.txt", 0);
		hSplit3("tables\\client.txt", 0);
		hSplit4("tables\\client.txt", 0);
		System.out.println("--------------------------------");
		System.out.println("SPLITTING : tables\\viewing.txt");
		hSplit1("tables\\viewing.txt", 0);
		hSplit2("tables\\viewing.txt", 0);
		hSplit3("tables\\viewing.txt", 0);
		hSplit4("tables\\viewing.txt", 0);
		System.out.println("--------------------------------");
		System.out.println("SPLITTING : tables\\orders.txt");
		hSplit1("tables\\orders.txt", 0);
		hSplit2("tables\\orders.txt", 0);
		hSplit3("tables\\orders.txt", 0);
		hSplit4("tables\\orders.txt", 0);
		System.out.println("--------------------------------");
		System.out.println("SPLITTING : tables\\parts.txt");
		hSplit1("tables\\parts.txt", 0);
		hSplit2("tables\\parts.txt", 0);
		hSplit3("tables\\parts.txt", 0);
		hSplit4("tables\\parts.txt", 0);
		System.out.println("--------------------------------");
		System.out.println("SPLITTING : tables\\odetails.txt");
		hSplit1("tables\\odetails.txt", 0);
		hSplit2("tables\\odetails.txt", 0);
		hSplit3("tables\\odetails.txt", 0);
		hSplit4("tables\\odetails.txt", 0);
		Utility.validate("outputFiles\\hSplitOut", "correctOutput\\hSplitCOutput",true);
	}
	
	public void hSplit1(String fileName, int joinKey) throws Exception {
		ThreadList.init();
		System.out.println();
		System.out.println("Split 1 : " + fileName);
		Connector readRToHSplit = new Connector("readRToHSplit");
		Connector hSplitToOut1 = new Connector("hSplitToOut1");
		Connector hSplitToOut2 = new Connector("hSplitToOut2");
		Connector hSplitToOut3 = new Connector("hSplitToOut3");
		Connector hSplitToOut4 = new Connector("hSplitToOut4");
		ReadRelation rRelation = new ReadRelation(fileName, readRToHSplit);
		HSplit hSplit = new HSplit(readRToHSplit, hSplitToOut1, hSplitToOut2, hSplitToOut3, hSplitToOut4, joinKey);
		Print printTuples1 = new Print(hSplitToOut1);
		Sink sink2 = new Sink(hSplitToOut2);
		Sink sink3 = new Sink(hSplitToOut3);
		Sink sink4 = new Sink(hSplitToOut4);
		ThreadList.run(printTuples1);
	}
	
	public void hSplit2(String fileName, int joinKey) throws Exception {
		ThreadList.init();
		System.out.println();
		System.out.println("Split 2 : " + fileName);
		Connector readRToHSplit = new Connector("readRToHSplit");
		Connector hSplitToOut1 = new Connector("hSplitToOut1");
		Connector hSplitToOut2 = new Connector("hSplitToOut2");
		Connector hSplitToOut3 = new Connector("hSplitToOut3");
		Connector hSplitToOut4 = new Connector("hSplitToOut4");
		ReadRelation rRelation = new ReadRelation(fileName, readRToHSplit);
		HSplit hSplit = new HSplit(readRToHSplit, hSplitToOut1, hSplitToOut2, hSplitToOut3, hSplitToOut4, joinKey);
		Print printTuples2 = new Print(hSplitToOut2);
		Sink sink1 = new Sink(hSplitToOut1);
		Sink sink3 = new Sink(hSplitToOut3);
		Sink sink4 = new Sink(hSplitToOut4);
		ThreadList.run(printTuples2);
	}
	
	public void hSplit3(String fileName, int joinKey) throws Exception {
		ThreadList.init();
		System.out.println();
		System.out.println("Split 3 : " + fileName);
		Connector readRToHSplit = new Connector("readRToHSplit");
		Connector hSplitToOut1 = new Connector("hSplitToOut1");
		Connector hSplitToOut2 = new Connector("hSplitToOut2");
		Connector hSplitToOut3 = new Connector("hSplitToOut3");
		Connector hSplitToOut4 = new Connector("hSplitToOut4");
		ReadRelation rRelation = new ReadRelation(fileName, readRToHSplit);
		HSplit hSplit = new HSplit(readRToHSplit, hSplitToOut1, hSplitToOut2, hSplitToOut3, hSplitToOut4, joinKey);
		Print printTuples3 = new Print(hSplitToOut3);
		Sink sink1 = new Sink(hSplitToOut1);
		Sink sink2 = new Sink(hSplitToOut2);
		Sink sink4 = new Sink(hSplitToOut4);
		ThreadList.run(printTuples3);
	}
	
	public void hSplit4(String fileName, int joinKey) throws Exception {
		ThreadList.init();
		System.out.println();
		System.out.println("Split 4 : " + fileName);
		Connector readRToHSplit = new Connector("readRToHSplit");
		Connector hSplitToOut1 = new Connector("hSplitToOut1");
		Connector hSplitToOut2 = new Connector("hSplitToOut2");
		Connector hSplitToOut3 = new Connector("hSplitToOut3");
		Connector hSplitToOut4 = new Connector("hSplitToOut4");
		ReadRelation rRelation = new ReadRelation(fileName, readRToHSplit);
		HSplit hSplit = new HSplit(readRToHSplit, hSplitToOut1, hSplitToOut2, hSplitToOut3, hSplitToOut4, joinKey);
		Print printTuples4 = new Print(hSplitToOut4);
		Sink sink1 = new Sink(hSplitToOut1);
		Sink sink2 = new Sink(hSplitToOut2);
		Sink sink3 = new Sink(hSplitToOut3);
		ThreadList.run(printTuples4);
	}
}