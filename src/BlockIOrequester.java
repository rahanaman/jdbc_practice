import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class BlockIOrequester {
	private static int BLOCK_SIZE = DBProgram.BLOCK_SIZE;
	private String fileID;
	
	private File file;
	
	public BlockIOrequester(String fileID) {
		this.fileID = fileID+".bin";
		file = new File(this.fileID);
	}
	
	public File getFile() {
		return file;
	}
	
	
	public void init() {
		Block block = new Block(new byte[] {-1});
		write(0,block.getContent());
		
	}
	
	public Block read(int offset){
		byte[] readByte = new byte[BLOCK_SIZE];
		try {
			
			RandomAccessFile fin = new RandomAccessFile(file, "r");
			fin.seek(offset*BLOCK_SIZE);
			fin.read(readByte,0,BLOCK_SIZE);
			fin.close();
			Block b = new Block(readByte);
			return b;
		} catch(IOException e) {
			e.printStackTrace();
		} catch (IndexOutOfBoundsException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void write(int offset, byte[] input) {
		try {
			RandomAccessFile fout = new RandomAccessFile(file, "rw");
			fout.seek(offset*BLOCK_SIZE);
			fout.write(input);
			fout.close();
			
		} catch(IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void write(int offset, Block input) {
		byte[] string = input.getContent();
		try {
			RandomAccessFile fout = new RandomAccessFile(file, "rw");
			fout.seek(offset*BLOCK_SIZE);
			fout.write(string);
			fout.close();
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
}
