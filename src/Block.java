import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Block {
	protected static int BLOCK_SIZE = DBProgram.BLOCK_SIZE;
	
	protected byte[] content;
	public byte[] getContent() {
		return content;
	}
	
	public Block() {
		content = new byte[BLOCK_SIZE];
		for(int i=0;i<BLOCK_SIZE;++i) {
			content[i] = 0;
		}
	}
	
	public Block(byte[] bytes) {
		this();
		for(int i=0;i<bytes.length;++i) {
			content[i] = bytes[i];
		}
	}
	
	public Block(int recordSize,String content) {
		this();
		write(0,recordSize,content);
	}
	
	public void write(int offset, int recordSize, String content){
		int index = offset * recordSize;
		byte[] string = content.getBytes();
		for(int i=0;i<string.length;++i) {
			this.content[i+index] = string[i];
		}
	}
	public void write(int offset, int recordSize, byte[] content){
		int index= offset * recordSize;
		for(int i=0;i<content.length;++i) {
			this.content[i+index] = content[i];
		}
	}
	
	public void erase(int offset, int recordSize) {
		int index= offset * recordSize;
		for(int i =0;i<recordSize;++i) {
			this.content[index +i] = 0;
		}
	}
	
	public void printData(int offset, List<AttributeMetadata> ls, int size) {
		int index = offset*size;
		int subindex =0;
		for(int i = 0; i<ls.size();++i) {
			System.out.print(new String(Arrays.copyOfRange(content, index+subindex, index+subindex+ls.get(i).GetValue()))+" ");
			subindex+=ls.get(i).GetValue();
		}
		System.out.println("");
	}
	public void printDataNoln(int offset, List<AttributeMetadata> ls, int size) {
		int index = offset*size;
		int subindex =0;
		for(int i = 0; i<ls.size();++i) {
			System.out.print(new String(Arrays.copyOfRange(content, index+subindex, index+subindex+ls.get(i).GetValue()))+" ");
			subindex+=ls.get(i).GetValue();
		}
	}
	
	
	public void printData(List<String> data) {
		for(int i=0;i<data.size();++i) {
			System.out.print(data.get(i)+" ");
		}
		System.out.println("");
	}
	
	public List<String> getData(int offset, List<AttributeMetadata> ls, int size) {
		int index = offset*size;
		int subindex =0;
		List<String> list = new ArrayList<String>();
		for(int i = 0; i<ls.size();++i) {
			list.add(new String(Arrays.copyOfRange(content, index+subindex, index+subindex+ls.get(i).GetValue())));
			subindex+=ls.get(i).GetValue();
		}
		return list;
	}
	
	
}
