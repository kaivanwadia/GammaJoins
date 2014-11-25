package gammajoins;

import gammaSupport.ThreadList;
import RegTest.Utility;
import basicConnector.Connector;

public class Main {
	public static void main(String args[]) throws Exception {
		ThreadList.init();
		Connector readT1ToBloom = new Connector("readT1ToBloom");
		ReadRelation rRelation1 = new ReadRelation("tables/orders.txt", readT1ToBloom);
		Connector readT2ToBloomFilter = new Connector("readT2ToBloomFilter");
		ReadRelation rRelation2 = new ReadRelation("tables/odetails.txt", readT2ToBloomFilter);
		Connector bloomToPrintTuple = new Connector("bloomTout1");
		Connector bloomToPrintMap = new Connector("bloomBmapout1");
		Bloom bloom1 = new Bloom(readT1ToBloom, bloomToPrintTuple, bloomToPrintMap, 0);
		Connector bloomFilterToPrint = new Connector("bloomFilterToPrint");
		BloomFilter bloomFilter1 = new BloomFilter(readT2ToBloomFilter, bloomToPrintMap, bloomFilterToPrint, 0);
		Sink sinkT1Tuple = new Sink(bloomToPrintTuple);
		Print printTuples = new Print(bloomFilterToPrint);
		ThreadList.run(printTuples);
	}
}
