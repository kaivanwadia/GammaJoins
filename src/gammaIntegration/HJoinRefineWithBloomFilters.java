package gammaIntegration;

import gammaSupport.ArrayConnectors;
import gammaSupport.ThreadList;
import gammajoins.Bloom;
import gammajoins.HSplit;
import gammajoins.Merge;
import gammajoins.MergeM;
import basicConnector.Connector;

public class HJoinRefineWithBloomFilters extends ArrayConnectors{
	
	public HJoinRefineWithBloomFilters(Connector in1, int joinKey, Connector mergeOut, Connector mmergeOut){
		
		Connector[] outs = ArrayConnectors.initConnectorArray("out");
		Connector[] bloomT = ArrayConnectors.initConnectorArray("bloomT");
		Connector[] bloomM = ArrayConnectors.initConnectorArray("bloomM");
		
		
		HSplit hSplit = new HSplit(in1, outs[0], outs[1], outs[2], outs[3], joinKey);
		
		Bloom b1 = new Bloom(outs[0], bloomT[0], bloomM[0], joinKey);
		Bloom b2 = new Bloom(outs[1], bloomT[1], bloomM[1], joinKey);
		Bloom b3 = new Bloom(outs[2], bloomT[2], bloomM[2], joinKey);
		Bloom b4 = new Bloom(outs[3], bloomT[3], bloomM[3], joinKey);
		
		Merge merge = new Merge(bloomT[0], bloomT[1], bloomT[2], bloomT[3], mergeOut);
		MergeM mmerge = new MergeM(bloomM[0], bloomM[1], bloomM[2], bloomM[3], mmergeOut);
		
	}
}
