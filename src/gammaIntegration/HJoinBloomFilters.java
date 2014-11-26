package gammaIntegration;

import basicConnector.Connector;

public class HJoinBloomFilters {

	public HJoinBloomFilters(Connector aIn, Connector bIn, Connector out, int joinKey){
		Connector mergeOutA = new Connector("mergeOutA");
		Connector mmergeOutA = new Connector("mmergeOutA");
		MapReduceBloom mapReduceBloom = new MapReduceBloom(aIn, joinKey, mergeOutA, mmergeOutA);
		
		Connector bOut = new Connector("bOut");
		MapReduceBFilter mapReduceBFilter = new MapReduceBFilter(mmergeOutA, bIn, joinKey, bOut);
		
		Connector finalOut = new Connector("finalOut");
		MapReduceHJoin mapReduceHJoin = new MapReduceHJoin();
	}
	
}
