import java.util.ArrayList;
import java.util.List;

public class RelationMetadata {
	private String name;
	private List<AttributeMetadata> attributes;
	
	public RelationMetadata(String name) {
		this.name = name;
		attributes = new ArrayList<AttributeMetadata>();
	}
	
	public String getName() {
		return name;
	}
	
	public List<AttributeMetadata> GetAttributes(){
		return attributes;
	}
	
	public int getAttNum() {
		return attributes.size();
	}
	
	public int getAttVal(String att) {
		for(int i= 0;i<attributes.size();++i) {
			if(attributes.get(i).GetName().equals(att)) {
				return attributes.get(i).GetValue();
			}
		}
		return 0;
	}
	
	public void addAttribute(String name, int value) {
		AttributeMetadata data = new AttributeMetadata(name, value);
		attributes.add(data);
	}
}
