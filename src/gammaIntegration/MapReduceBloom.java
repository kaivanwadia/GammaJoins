package gammaIntegration;

import gammaSupport.ArrayConnectors;
import gammaSupport.ThreadList;
import gammajoins.Bloom;
import gammajoins.HSplit;
import gammajoins.Merge;
import gammajoins.MergeM;
import basicConnector.Connector;

public class MapReduceBloom extends ArrayConnectors{
	
	
	public MapReduceBloom(Connector aTuplesIn, int joinKey, Connector aTuplesOut, Connector mapOut){
		
		Connector[] hSplitToBloom = ArrayConnectors.initConnectorArray("out");
		Connector[] bloomToMerge = ArrayConnectors.initConnectorArray("bloomT");
		Connector[] bloomToMapMerge = ArrayConnectors.initConnectorArray("bloomM");
		
		
		HSplit hSplit = new HSplit(aTuplesIn, hSplitToBloom[0], hSplitToBloom[1], hSplitToBloom[2], hSplitToBloom[3], joinKey);
		
		Bloom b1 = new Bloom(hSplitToBloom[0], bloomToMerge[0], bloomToMapMerge[0], joinKey);
		Bloom b2 = new Bloom(hSplitToBloom[1], bloomToMerge[1], bloomToMapMerge[1], joinKey);
		Bloom b3 = new Bloom(hSplitToBloom[2], bloomToMerge[2], bloomToMapMerge[2], joinKey);
		Bloom b4 = new Bloom(hSplitToBloom[3], bloomToMerge[3], bloomToMapMerge[3], joinKey);
		
		Merge merge = new Merge(bloomToMerge[0], bloomToMerge[1], bloomToMerge[2], bloomToMerge[3], aTuplesOut);
		MergeM mmerge = new MergeM(bloomToMapMerge[0], bloomToMapMerge[1], bloomToMapMerge[2], bloomToMapMerge[3], mapOut);
	}
}
