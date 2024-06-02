
public class JoinBlock extends Block {
	private int blockingfactor;
	
	private int num;
	
	public JoinBlock( int blockingfactor) {
		super();
		num =0;
		this.blockingfactor = blockingfactor;
	}
	
	public void write(int recordSize, String content) {
		super.write(num, recordSize, content);
		num++;
	}
	
	
	public boolean isFull() {
		return num >= blockingfactor;
	}
	public boolean isNull() {
		return (num ==0);
	}
	public int getNum() {
		return num;
	}
	

}
