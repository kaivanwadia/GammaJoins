package tests;

import static org.junit.Assert.*;
import gammaIntegration.Gamma;
import gammaSupport.ThreadList;
import gammajoins.Print;
import gammajoins.ReadRelation;

import org.junit.Test;

import RegTest.Utility;
import basicConnector.Connector;

public class GammaTest {

	@Test
	public void gammaTest1() throws Exception {
		Utility.redirectStdOut("outputFiles\\gammaOut");
		gamma("tables\\client.txt", "tables\\viewing.txt", 0, 0);
		System.out.println();
		gamma("tables\\orders.txt", "tables\\odetails.txt", 0, 0);
		System.out.println();
		gamma("tables\\parts.txt", "tables\\odetails.txt", 0, 1);
		Utility.validate("outputFiles\\gammaOut", "correctOutput\\gammaCOutput", true);
	}
	
	public void gamma (String fileNameA, String fileNameB, int aJoinKey, int bJoinKey) throws Exception {
		ThreadList.init();
		System.out.println("INNER JOIN OF " + fileNameA + " and " + fileNameB);
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
