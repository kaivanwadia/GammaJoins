package gammaIntegration;

import basicConnector.Connector;

public class HJoinBloomFilters {

	public HJoinBloomFilters(Connector aIn, Connector bIn, Connector out, int joinKeyA, int joinKeyB){
		Connector mergeOutA = new Connector("mergeOutA");
		Connector mmergeOutA = new Connector("mmergeOutA");
		MapReduceBloom mapReduceBloom = new MapReduceBloom(aIn, joinKeyA, mergeOutA, mmergeOutA);
		
		Connector bOut = new Connector("bOut");
		MapReduceBFilter mapReduceBFilter = new MapReduceBFilter(mmergeOutA, bIn, joinKeyB, bOut);
		
		MapReduceHJoin mapReduceHJoin = new MapReduceHJoin(mergeOutA, bOut, out, joinKeyA, joinKeyB);
		
		
	}
	
}
