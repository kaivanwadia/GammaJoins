package gammajoins;

import gammaSupport.ThreadList;
import gammaSupport.Tuple;

import java.util.ArrayList;
import java.util.HashMap;

import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class Hjoin extends Thread {
	ReadEnd a,b;
	WriteEnd out;
	int aCol, bCol;
	public Hjoin(ReadEnd a, ReadEnd b, int aCol, int bCol, WriteEnd out)
	{
		this.a = a;
		this.b = b;
		this.out = out;
		this.aCol = aCol;
		this.bCol = bCol;
		
		ThreadList.add(this);
	}
	
	public void run(){
		HashMap<String,ArrayList<Tuple> > mapA = new HashMap<String, ArrayList<Tuple>>();
		try{
			Tuple tuple = a.getNextTuple();
			while(tuple != null){
				if(mapA.get(tuple.get(aCol)) == null)
				{
					ArrayList<Tuple> list = new ArrayList<Tuple>();
					list.add(tuple);
					mapA.put(tuple.get(aCol), list);
				}else{
					ArrayList<Tuple> list = mapA.get(aCol);
					list.add(tuple);
					mapA.put(tuple.get(aCol), list);
				}
				tuple = a.getNextTuple();
			}
			
			tuple = b.getNextTuple();
			while(tuple != null){
				if(mapA.get(tuple.get(bCol)) != null){
					ArrayList<Tuple> list = mapA.get(tuple.get(bCol));
					for(int i = 0; i < list.size(); i++){
						out.putNextTuple(Tuple.join(list.get(i), tuple, aCol, bCol));
					}
				}
				tuple = b.getNextTuple();
			}
			
		}
		catch (Exception e)
		{
			
		}
		
		
	}

}
