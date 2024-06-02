import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TempFileManager {
	private final int MOD = DBProgram.MOD; 
	private final int BLOCK_SIZE = DBProgram.BLOCK_SIZE;
	private int size;
	private String name;
	private JoinBlock[] blockList = new JoinBlock[MOD];
	private BlockIOrequester[] ios = new BlockIOrequester[MOD];
	private BlockIOrequester io;
	private int blockingfactor;
	private Block tempBlock;
	private RelationMetadata meta;
	private int index;
	private int[] offsets = {0,0,0,0,0};
	private Map<String,List<Integer>> hash;
	
	public int[] getOffsets() {
		return offsets;
	}
	
	public Block getBlock(int num,int offset) {
		return ios[num].read(offset);
	}
	
	
	
	public List<Integer> getHash(String s){
		if(hash.containsKey(s)) {
			return hash.get(s);
		}
		return null;
	}
	public int getIndex() {
		return index;
	}
	
	public TempFileManager(String name,RelationMetadata meta, int blockingfactor, int size ,String attribute) {
		index = meta.getIndex(attribute);
		this.name = name;
		this.meta = meta;
		for(int i=0;i<MOD;++i) {
			blockList[i] = new JoinBlock(blockingfactor);
			ios[i] = new BlockIOrequester(name+i+"");
			ios[i].init();
		}
		hash = new HashMap<String,List<Integer>>();
		this.size = size;
		this.io = new BlockIOrequester(name);
		this.blockingfactor=blockingfactor;
		tempBlock = new Block();
		buildTempFile();
	}
	
	public void buildTempFile() {
		int len = (int) io.getFile().length();
		int num = len/BLOCK_SIZE;
		tempBlock = io.read(0);
		if((tempBlock.getContent())[0]==(byte)-1) {
			//no free list
			noFreeList(num);
		}
		else {
			FreeList(num);
		}
		for(int i=0;i<MOD;++i) {
			if(!blockList[i].isNull()) {
				ios[i].write(offsets[i],blockList[i]);
			}
		}
		
		return;
	}

	private void noFreeList(int num) {
		List<List<String>> list = new ArrayList<>();
		
		for(int i=0;i<num;++i) {
			tempBlock = io.read(i);
			for(int j =0;j<blockingfactor;++j) {
				if((tempBlock.getContent())[j*size]==0||(tempBlock.getContent())[j*size]==-1) {
					continue;
				}
				List<String> ls = tempBlock.getData(j, meta.getAttributes(), size);
				String s = ls.get(index);
				//System.out.println(s);
				int n = calc(s);
				n = n%5;
				blockList[n].write(size,getString(ls));;
				if(!hash.containsKey(s)) {
					hash.put(s, new ArrayList());
				}
				//System.out.println(offsets[n]*blockingfactor + blockList[n].getNum());
				hash.get(s).add(offsets[n]*blockingfactor + blockList[n].getNum()-1);
				if(blockList[n].isFull()) {
					ios[n].write(offsets[n],blockList[n]);
					offsets[n]++;
					blockList[n] = new JoinBlock(blockingfactor);
				}
			}
			
		}
		return;
	}
	
	private void FreeList(int num) {
		List<List<String>> list = new ArrayList<>();
		int offset;
		int cnt =0;		
		tempBlock = io.read(0);
		offset = getOffset(0);
		for(int i=1;i<blockingfactor;++i) {
			cnt++;
			if(offset == cnt) {
				offset = getOffset(i*size);
				cnt=0;
				continue;
			}
			if((tempBlock.getContent())[i*size]==0||(tempBlock.getContent())[i*size]==-1) {
				continue;
			}
			List<String> ls = tempBlock.getData(i, meta.getAttributes(), size);
			String s = ls.get(index);
			int n = calc(s);
			n = n%5;
			blockList[n].write(size,getString(ls));;
			if(!hash.containsKey(s)) {
				hash.put(s, new ArrayList());
			}
			hash.get(s).add(offsets[n]*blockingfactor + blockList[n].getNum()-1);
			if(blockList[n].isFull()) {
				ios[n].write(offsets[n],blockList[n]);
				offsets[n]++;
				blockList[n] = new JoinBlock(blockingfactor);
			}
			
		}
		
		for(int i=1;i<num;++i) {
			tempBlock = io.read(i);
			for(int j= 0;j<blockingfactor;++j) {
				cnt++;
				if(offset == cnt) {
					offset = getOffset(j*size);
					cnt =0;
					continue;
				}
				if((tempBlock.getContent())[j*size]==0||(tempBlock.getContent())[j*size]==-1) {
					continue;
				}
				List<String> ls = tempBlock.getData(j, meta.getAttributes(), size);
				String s = ls.get(index);
				int n = calc(s);
				n = n%5;
				blockList[n].write(size,getString(ls));;
				if(!hash.containsKey(s)) {
					hash.put(s, new ArrayList());
				}
				hash.get(s).add(offsets[n]*blockingfactor + blockList[n].getNum()-1);
				if(blockList[n].isFull()) {
					ios[n].write(offsets[n],blockList[n]);
					offsets[n]++;
					blockList[n] = new JoinBlock(blockingfactor);
				}
			}
		}
		
		return;

	}
	
	private int calc(String s) {
		int n =0;
		for(int i=0;i<s.length();++i) {
			n += (int)s.charAt(i);
		}
		
		return n;
	}
	
	private String getString(List<String> list) {
		String re = new String("");
		for(int i=0;i<list.size();++i) {
			re = re+list.get(i);
		}
		return re;
	}
	
	private byte getOffset(int index) {
		return (tempBlock.getContent())[index];
	}
	
	public void quit() {
		for(int i= 0;i<MOD;++i) {
			ios[i].getFile().delete();
		}
	}
	
	
}
