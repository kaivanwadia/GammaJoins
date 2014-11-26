package gammajoins;

import gammaIntegration.Gamma;
import gammaIntegration.MapReduceBloom;
import gammaIntegration.HJoinBloomFilters;
import gammaSupport.Relation;
import gammaSupport.ThreadList;
import basicConnector.Connector;

public class Main {
	public static void main(String args[]) throws Exception {
		String fileNameA = "tables/client.txt";
		String fileNameB = "tables/viewing.txt";
		int aJoinKey = 0;
		int bJoinKey = 0;
		ThreadList.init();
		Connector aToGamma = new Connector("aToGamma");
		Connector bToGamma = new Connector("bToGamma");
		Connector output = new Connector("output");
		ReadRelation readA = new ReadRelation(fileNameA, aToGamma);
		ReadRelation readB = new ReadRelation(fileNameB, bToGamma);
		Gamma gamma = new Gamma(aToGamma, bToGamma, aJoinKey, bJoinKey, output);
		Print print = new Print(output);
		ThreadList.run(print);
	}
}
