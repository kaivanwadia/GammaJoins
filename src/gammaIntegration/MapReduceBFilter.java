package gammaIntegration;

import gammaSupport.ArrayConnectors;
import gammajoins.BloomFilter;
import gammajoins.HSplit;
import gammajoins.Merge;
import gammajoins.SplitM;
import basicConnector.Connector;

public class MapReduceBFilter extends ArrayConnectors {
	
	public MapReduceBFilter(Connector mapIn, Connector bTuplesIn, int joinKey, Connector bTuplesOut) {
		Connector[] mapsToBFilter = ArrayConnectors.initConnectorArray("map");
		Connector[] bTuplesToBFilter = ArrayConnectors.initConnectorArray("bTuples");
		
		SplitM splitM = new SplitM(mapIn, mapsToBFilter[0], mapsToBFilter[1], mapsToBFilter[2], mapsToBFilter[3]);
		HSplit splitB = new HSplit(bTuplesIn, bTuplesToBFilter[0], bTuplesToBFilter[1], bTuplesToBFilter[2], bTuplesToBFilter[3], joinKey);
		
		Connector[] bFilterToMerge = ArrayConnectors.initConnectorArray("bFilterOut");
		
		BloomFilter bFilter0 = new BloomFilter(bTuplesToBFilter[0], mapsToBFilter[0], bFilterToMerge[0], joinKey);
		BloomFilter bFilter1 = new BloomFilter(bTuplesToBFilter[1], mapsToBFilter[1], bFilterToMerge[1], joinKey);
		BloomFilter bFilter2 = new BloomFilter(bTuplesToBFilter[2], mapsToBFilter[2], bFilterToMerge[2], joinKey);
		BloomFilter bFilter3 = new BloomFilter(bTuplesToBFilter[3], mapsToBFilter[3], bFilterToMerge[3], joinKey);
		
		Merge merge = new Merge(bFilterToMerge[0], bFilterToMerge[1], bFilterToMerge[2], bFilterToMerge[3], bTuplesOut);
	}

}
