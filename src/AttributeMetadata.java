
public class AttributeMetadata {
	private String name;
	private int value;
	public AttributeMetadata(String name, int value) {
		this.name = name;
		this.value = value;
	}
	public String GetName() {
		return name;
	}
	public int GetValue() {
		return value;
	}
}
