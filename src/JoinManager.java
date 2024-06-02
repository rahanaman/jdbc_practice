import java.util.ArrayList;
import java.util.List;

public class JoinManager {
	private final int MOD = DBProgram.MOD;
	private final int BLOCK_SIZE = DBProgram.BLOCK_SIZE;
	private RelationMetadata meta1;
	private RelationMetadata meta2;
	private RelationManager r1;
	private RelationManager r2;
	private String input1;
	private String input2;
	private int size1;
	private int size2;
	private int blockingfactor1;
	private int blockingfactor2;
	private TempFileManager t1;
	private TempFileManager t2;
	
	public JoinManager(String input1, String input2) {
		this.input1 = input1;
		this.input2 = input2;
		meta1 = SQLRequester.Instance().getRelationData(input1);
		meta2 = SQLRequester.Instance().getRelationData(input2); 
		List<AttributeMetadata> ls = meta1.getAttributes();
		size1 =0;
		int num = ls.size();
		for(int i=0;i<num;++i){
			size1 += ls.get(i).GetValue();
		}
		blockingfactor1 = BLOCK_SIZE/size1;
		
		ls = meta2.getAttributes();
		size2 =0;
		num = ls.size();
		for(int i=0;i<num;++i){
			size2 += ls.get(i).GetValue();
		}
		blockingfactor2 = BLOCK_SIZE/size2;
		Join();
	}
	
	public String findJoin() {
		List<AttributeMetadata> list = meta1.getAttributes();
		List<String> atts = new ArrayList();
		for(int i=0;i<list.size();++i) {
			atts.add(list.get(i).GetName());
		}
		list = meta2.getAttributes();
		for(int i=0;i<list.size();++i) {
			if(atts.contains(list.get(i).GetName())) {
				return list.get(i).GetName();
			}
		}
		return null;
	}
	
	public void Join() {
		String join = findJoin();
		if(join == null) return;
		t1 = new TempFileManager(input1, meta1,  blockingfactor1,  size1 , join);
		t2 = new TempFileManager(input2,meta2, blockingfactor2, size2,join);
		int index = t2.getIndex();
		int[] offsets = t2.getOffsets();
		
		for(int i=0;i<MOD;++i) {
			
			Block b = t2.getBlock(i, 0);
			if((b.getContent())[0] == (byte)-1) continue;
			for(int j =0;j<=offsets[i];++j) {
				
				Block block = t2.getBlock(i, j);
				for(int k =0;k<blockingfactor2;++k) {
					if(block.getContent()[k*size2] == (byte)0) break;
					List<String> list = block.getData(k, meta2.getAttributes(), size2);
					
					List<Integer> offs =t1.getHash(list.get(index));
					if(offs!=null) {
						for(int l=0;l<offs.size();++l) {
							int offset = offs.get(l);
							Block tempBlock = t1.getBlock(i, (offset/blockingfactor1));
							block.printDataNoln(k, meta2.getAttributes(), size2);
							tempBlock.printData(offset%blockingfactor1, meta1.getAttributes(), size1);
						}
					}
				}
				
			}
		}
//		t1.quit();
//		t2.quit();
	}
}
