import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;



public class SQLRequester {
	
	private static SQLRequester instance;
	
	public static SQLRequester Instance() {
		if(instance == null) instance = new SQLRequester();
		return instance;
	}
	
	static final String DB_URL =
			"jdbc:mysql://localhost:3306/boardgame?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	
	static final String USER = "root"; // user name
	static final String PASS = "3942hyun**"; // user password
	
	static final String RELATION_INSERT = "insert into relation_metadata value (?,?)";
	static final String ATT_INSERT = "insert into attribute_metadata value (?,?,?,?)";
	static final String RELATION_QUERY = "select * from relation_metadata";
	static final String META_QUERY ="select * from attribute_metadata where relation_name = ";
	static final String RELATION_DEL = "delete from relation_metadata where relation_name = ";
	static final String ATTRIBUTE_DEL = "delete from attribute_metadata where relation_name = ";
	
	//relation existence check
	public boolean isRelationExist(String name) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try { 
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select count(*) from relation_metadata"+ " where relation_name = '"+name+"'");
			if(rs.next()) return !(rs.getInt(1)==0);
		}catch (SQLException e) {
			System.out.println("SQLException : " + e);
		} finally {
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return true;
	}
	
	//call relation meta data
	public RelationMetadata getRelationData(String name){
		RelationMetadata metadata = new RelationMetadata(name);
		String metaQuery = META_QUERY +"'"+ name+"'" + " order by id";
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
			
		try { 
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(metaQuery);
			
			
			while (rs.next()) {
				metadata.addAttribute(rs.getString("attribute_name"), rs.getInt("length"));
			}
			return metadata;
		}
		
		catch (SQLException e) {
			System.out.println("SQLException : " + e);
		} finally {
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return metadata;
	}
	
	// get relation list
	public List<String> getRelationList() {
		List<String> relationList = new ArrayList<String>();
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
			
		try { 
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(RELATION_QUERY);

			while (rs.next()) {
				relationList.add(rs.getString("relation_name"));
			}
		}catch (SQLException e) {
			System.out.println("SQLException : " + e);
		} finally {
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return relationList;
	}
	
	//insert
	public void insertMeta(RelationMetadata meta) {
		insertRelation(meta.getName(),meta.getAttNum());
		int num = meta.getAttNum();
		List<AttributeMetadata> atts = meta.getAttributes();
		for(int i = 0;i<num;++i) {
			AttributeMetadata data = atts.get(i);
			insertAtt(meta.getName(), data.GetName(), data.GetValue(), i);
		}
	}
	
	private void insertAtt(String relationName, String attName, int length, int index) {
		Connection conn = null;
		PreparedStatement stmt = null;
			
		try { 
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.prepareStatement(ATT_INSERT);
			stmt.setString(1, relationName);
			stmt.setString(2, attName);
			stmt.setInt(3, length);
			stmt.setInt(4, index);
			stmt.executeUpdate();
			
			
		}catch (SQLException e) {
			System.out.println("SQLException : " + e);
		} finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void insertRelation(String name, int number) {
		Connection conn = null;
		PreparedStatement stmt = null;
			
		try { 
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.prepareStatement(RELATION_INSERT);
			stmt.setString(1, name);
			stmt.setInt(2, number);
			stmt.executeUpdate();
			
			
		}catch (SQLException e) {
			System.out.println("SQLException : " + e);
		} finally {
			try {
				
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	//drop relation
	
	public void dropReation(String name) {
		Connection conn = null;
		PreparedStatement stmt = null;
		PreparedStatement stmt2 = null;
		String q1 = RELATION_DEL + "'"+name+"'";
		String q2 = ATTRIBUTE_DEL+"'"+name+"'";
		try { 
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.prepareStatement(q1);

			stmt.executeUpdate();
			stmt2 = conn.prepareStatement(q2);

			stmt2.executeUpdate();
			
			
		}catch (SQLException e) {
			System.out.println("SQLException : " + e);
		} finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
	
}


