import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class RelationManager {
	private static int BLOCK_SIZE = DBProgram.BLOCK_SIZE;
	private int size = 0;
	private RelationMetadata meta;
	private File file;
	private BlockIOrequester io;
	private Block block;
	private int blockingfactor;
	private Scanner scanner = new Scanner(System.in);
	
	public RelationManager(RelationMetadata data) {
		meta = data;
		io = new BlockIOrequester(data.getName());
		List<AttributeMetadata> ls = data.GetAttributes();
		file = io.getFile();
		int num = ls.size();
		for(int i=0;i<num;++i){
			size += ls.get(i).GetValue();
		}
		blockingfactor = BLOCK_SIZE/size;
	}
	//insert
	public void insert() {
		byte[] data = getRecordDataInput(); 
		insertRecord(data);
	}
	//select all
	public void listAll() {
		block = io.read(0);
		if((block.getContent())[0]==(byte)-1) {
			//no free list
			noFreeList();
			return;
		}
		FreeList();
	}
	//delete
	public void delete() {
		block = io.read(0);
		String pk = getDelData();
		if((block.getContent())[0] == (byte)-1){
			deleteNoFree(pk);
			return;
		}
		deleteFree(pk);
	}
	//search
	public void search() {
		block = io.read(0);
		String pk = getDelData();
		if((block.getContent())[0] == (byte)-1){
			searchNoFree(pk);
			return;
		}
		searchFree(pk);
	}
	
	
	//insert implementation
	private void insertRecord(byte[] data){
		block = io.read(0);
		if((block.getContent())[0]==(byte)-1) {
			insertNoFree(data);
			return;
		}
		insertFree(data);
	}
	
	private void insertNoFree(byte[] data){
		if(checkNoFree(data)) {
			return;
		}
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		int index = findEntry();
		if(index == -1) {
			Block b = new Block();
			b.write(0,size,data);
			io.write(num, b);
		}
		else {
			block.write(index, size, data);
			io.write(num-1,block);
		}
	}
	
	private boolean checkNoFree(byte[] data) {
		String pk = new String(Arrays.copyOfRange(data, 0, meta.GetAttributes().get(0).GetValue()));
		int cnt = 0;
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		for(int i=0;i<num;++i) {
			block = io.read(i);
			for(int j=0;j<blockingfactor;++j) {
				if((block.getContent())[j*size] == -1||(block.getContent())[j*size] == 0) {
					continue;
				}
				cnt++;
				List<String> rec = block.getData(j, meta.GetAttributes(), size);
				if(rec.get(0).equals(pk)) {
					System.out.println("ERROR : primary key alread exists.");
					return true;
				}
				
			}
		}
		return false;
	}
	
	private void insertFree(byte[] data) {
		if(checkFree(data))return;
		int offset;
		int cnt =0;
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		int free =0;
		block = io.read(0);
		offset = getOffset(0);
		for(int i=1;i<blockingfactor;++i) {
			cnt++;
			if(offset == cnt) {
				offset = getOffset(i*size);
				cnt=0;
				
				if(offset ==0) {
					block.write(i, size, data);
					io.write(0,block);
					eraseFreeList(free);
					return;
				}
				free++;
				continue;
			}
			
		}
		
		for(int i=1;i<num;++i) {
			block = io.read(i);
			for(int j= 0;j<blockingfactor;++j) {
				cnt++;
				if(offset == cnt) {
					offset = getOffset(j*size);
					cnt =0;
					
					if(offset ==0) {
						block.write(j, size, data);
						io.write(i,block);
						eraseFreeList(free);
						return;
					}
					free++;
					continue;
				}
			}
		}
	}
	
	private boolean checkFree(byte[] data) {
		String pk = new String(Arrays.copyOfRange(data, 0, meta.GetAttributes().get(0).GetValue()));
		int offset;
		int cnt =0;
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		block = io.read(0);
		offset = getOffset(0);
		for(int i=1;i<blockingfactor;++i) {
			cnt++;
			if(offset == cnt) {  
				offset = getOffset(i*size);
				cnt=0;
				continue;
			}
			List<String> rec = block.getData(i, meta.GetAttributes(), size);
			if(rec.get(0).equals(pk)) {
				System.out.println("ERROR : primary key alread exists.");
				return true;
			}
		}
		
		for(int i=1;i<num;++i) {
			block = io.read(i);
			for(int j= 0;j<blockingfactor;++j) {
				cnt++;
				if(offset == cnt) {
					offset = getOffset(j*size);
					cnt =0;
					continue;
				}
				List<String> rec = block.getData(j, meta.GetAttributes(), size);
				if(rec.get(0).equals(pk)) {
					System.out.println("ERROR : primary key alread exists.");
					return true;
				}
			}
		}
		return false;
	}
	
	private void eraseFreeList(int freenum) {
		int offset;
		int cnt =0;
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		int free =0;
		if(freenum ==0) {
			block = io.read(0);
			block.write(0, size, new byte[] {(byte)-1});
			io.write(0, block);
			return;
		}
		block = io.read(0);
		offset = getOffset(0);
		
		for(int i=1;i<blockingfactor;++i) {
			cnt++;
			if(offset == cnt) {
				offset = getOffset(i*size);
				cnt=0;
				free++;
				if(free == freenum) {
					block.write(i, size, new byte[] {(byte)0});
					io.write(0,block);
					return;
				}
				continue;
			}
			
		}
		
		for(int i=1;i<num;++i) {
			block = io.read(i);
			for(int j= 0;j<blockingfactor;++j) {
				cnt++;
				if(offset == cnt) {
					offset = getOffset(j*size);
					cnt =0;
					free++;
					if(free == freenum) {
						block.write(j, size, new byte[] {(byte)0});
						io.write(i, block);
						return;
					} 
					continue;
				}
			}
		}
	}
	
	private byte[] getRecordDataInput() {
		byte[] data = new byte[size];
		for(int i=0;i<size;++i) {
			data[i] = (byte)32;
		}
		int index =0;
		for(int j=0;j<meta.getAttNum();++j) {
			System.out.println((meta.GetAttributes()).get(j).GetName()+"?");
			byte[] input = (scanner.nextLine()).getBytes();
			for(int i=0;i<input.length;++i) {
				data[index+i] = input[i];
			}
			index += meta.GetAttributes().get(j).GetValue();
		}
		
		return data;
	}
	
	private byte getOffset(int index) {
		return (block.getContent())[index];
	}
	
	private int findEntry() {
		int i = 0;
		int num = (int)(file.length())/BLOCK_SIZE;
		block = io.read(num-1);
		while(i<(BLOCK_SIZE/size)) {
			if((block.getContent())[i*size]==0) {
				return i;
			}
			i++;
		}
		return -1;
		
	}

	
	//select all implementation

	private void noFreeList() {
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		for(int i=0;i<num;++i) {
			block = io.read(i);
			for(int j =0;j<blockingfactor;++j) {
				if((block.getContent())[j*size]==0||(block.getContent())[j*size]==-1) {
					continue;
				}
				List<String> ls = block.getData(j, meta.GetAttributes(), size);
				for(int ind = 0; ind<ls.size();++ind) {
					System.out.print(ls.get(ind)+" ");
				}
				System.out.println("");
			}
			
		}
	}
	
	private void FreeList() {
		int offset;
		int cnt =0;
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		block = io.read(0);
		offset = getOffset(0);
		for(int i=1;i<blockingfactor;++i) {
			cnt++;
			if(offset == cnt) {
				offset = getOffset(i*size);
				cnt=0;
				continue;
			}
			if((block.getContent())[i*size]==0||(block.getContent())[i*size]==-1) {
				continue;
			}
			List<String> ls = block.getData(i, meta.GetAttributes(), size);
			for(int ind = 0; ind<ls.size();++ind) {
				System.out.print(ls.get(ind)+" ");
			}
			System.out.println("");
		}
		
		for(int i=1;i<num;++i) {
			block = io.read(i);
			for(int j= 0;j<blockingfactor;++j) {
				cnt++;
				if(offset == cnt) {
					offset = getOffset(j*size);
					cnt =0;
					continue;
				}
				if((block.getContent())[j*size]==0||(block.getContent())[j*size]==-1) {
					continue;
				}
				List<String> ls = block.getData(j, meta.GetAttributes(), size);
				for(int ind = 0; ind<ls.size();++ind) {
					System.out.print(ls.get(ind)+" ");
				}
				System.out.println("");
			}
		}
		
		
	}

	
	//search implementation
	
	private void searchNoFree(String pk) {

		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		for(int i=0;i<num;++i) {
			block = io.read(i);
			for(int j=0;j<blockingfactor;++j) {
				if((block.getContent())[j*size] == -1||(block.getContent())[j*size] == 0) {
					continue;
				}
				List<String> data = block.getData(j, meta.GetAttributes(), size);
				if(data.get(0).equals(pk)) {
					block.printData(j, meta.GetAttributes(), size);
					return;
				}
				
			}
		}
	}
	
	private void searchFree(String pk) {
		int offset;
		int cnt =0;
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		block = io.read(0);
		offset = getOffset(0);
		for(int i=1;i<blockingfactor;++i) {
			cnt++;
			if(offset == cnt) {  
				offset = getOffset(i*size);
				
				continue;
			}
			List<String> data = block.getData(i, meta.GetAttributes(), size);
			if(data.get(0).equals(pk)) {
				block.printData(i, meta.GetAttributes(), size);
				return;
			}
		}
		
		for(int i=1;i<num;++i) {
			block = io.read(i);
			for(int j= 0;j<blockingfactor;++j) {
				cnt++;
				if(offset == cnt) {
					offset = getOffset(j*size);
					cnt =0;
					continue;
				}
				List<String> data = block.getData(j, meta.GetAttributes(), size);
				if(data.get(0).equals(pk)) {
					block.printData(j, meta.GetAttributes(), size);
					return;
				}
			}
		}
	}
	
	//delete implementation
	
	public String getDelData() {
		System.out.println("Name?");
		byte[] data = new byte[meta.getAttVal("name")];
		
		for(int i=0;i<data.length;++i) {
			data[i] = (byte)32;
		}
		byte[] input = (scanner.nextLine()).getBytes();
		for(int i=0;i<input.length;++i) {
			data[i] = input[i];
		}
		return new String(data);
	}
	
	private void deleteNoFree(String pk) {
		int cnt =0;
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		for(int i=0;i<num;++i) {
			block = io.read(i);
			for(int j=0;j<blockingfactor;++j) {
				if((block.getContent())[j*size] == -1||(block.getContent())[j*size] == 0) {
					continue;
				}
				cnt++;
				List<String> data = block.getData(j, meta.GetAttributes(), size);
				if(data.get(0).equals(pk)) {
					block.write(j,size,new byte[] {(byte)0});
					io.write(i,block);
					block = io.read(0);
					block.write(0, size, new byte[] {(byte)cnt});
					io.write(0,block);
				}
				
			}
		}
	}
	
	private void deleteFree(String pk) {
		int offset;
		int cnt =0;
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		int free =0;
		block = io.read(0);
		offset = getOffset(0);
		for(int i=1;i<blockingfactor;++i) {
			cnt++;
			if(offset == cnt) {  
				offset = getOffset(i*size);
				cnt=0;
				free++;
				continue;
			}
			List<String> data = block.getData(i, meta.GetAttributes(), size);
			if(data.get(0).equals(pk)) {
				if(offset == 0) {
					block.write(i,size,new byte[] {(byte)0});
					io.write(0,block);
					changeFree(free,cnt);
					return;
				}
				else {
					block.write(i,size,new byte[] {(byte)(offset-cnt)});
					io.write(0,block);
					changeFree(free,cnt);
					return;
				}
			}
		}
		
		for(int i=1;i<num;++i) {
			block = io.read(i);
			for(int j= 0;j<blockingfactor;++j) {
				cnt++;
				if(offset == cnt) {
					offset = getOffset(j*size);
					cnt =0;
					free++;
					continue;
				}
				List<String> data = block.getData(j, meta.GetAttributes(), size);
				if(data.get(0).equals(pk)) {
					if(offset == 0) {
						block.write(j,size,new byte[] {(byte)0});
						io.write(i,block);
						changeFree(free,cnt);
						return;
					}
					else {
						block.write(j,size,new byte[] {(byte)(offset-cnt)});
						io.write(i,block);
						changeFree(free,cnt);
						return;
					}
				}
			}
		}
	}
	
	private void changeFree(int freenum, int data) {
		int offset;
		int cnt =0;
		int n = (int)file.length();
		int num = n/BLOCK_SIZE;
		int free =0;
		
		block = io.read(0);
		offset = getOffset(0);
		if(freenum ==0) {
			block.write(0, size,new byte[] {(byte)data});
			io.write(0, block);
			return;
		}
		for(int i=1;i<blockingfactor;++i) {
			cnt++;
			if(offset == cnt) {
				offset = getOffset(i*size);
				cnt=0;
				free++;
				if(free == freenum) {
					block.write(i, size, new byte[] {(byte)data});
					io.write(0,block);
					return;
				}
				continue;
			}
			
		}
		
		for(int i=1;i<num;++i) {
			block = io.read(i);
			for(int j= 0;j<blockingfactor;++j) {
				cnt++;
				if(offset == cnt) {
					offset = getOffset(j*size);
					cnt =0;
					free++;
					if(free == freenum) {
						block.write(j, size, new byte[] {(byte)data});
						io.write(i, block);
						return;
					} 
					continue;
				}
			}
		}
	}
	
	
	
}
