import java.io.File;
import java.util.List;
import java.util.Scanner;

//UI and JDBC connection
public class DBProgram {
	public static final int BLOCK_SIZE =100;
	public static final int MOD = 5;
	
	private Scanner scanner = new Scanner(System.in);
	private String input;
	
	public void init() {
		mainMenu();
	}
	private void mainMenu(){
		while(true) {
			System.out.println("====================================================");
			System.out.println("                      DB Program");
			System.out.println("====================================================");
			System.out.println("1. 릴레이션 관리 2. 릴레이션 생성 및 처리 3. 조인 결과 4. 종료");
			input = scanner.nextLine();
			switch(input) {
			case "1":
				relationMang();
				break;
			case "2":
				relationCnD();
				break;
			case"3":
				JoinMang();
				break;
			case "4":
				System.out.println("Program exit.");
				return;
			default:
				System.out.println("Invalid Input");
				break;
			}
		}
		
	}
	
	private void JoinMang() {
		System.out.println("====================================================");
		System.out.println("                       Join DB");
		System.out.println("====================================================");
		System.out.println("조인연산을 수행할 두 개의 릴레이션을 선택하세요.");
		List<String> ls = SQLRequester.Instance().getRelationList();
		String input1;
		String input2;
		for(int i=0;i<ls.size();++i) {
			System.out.println(i+" : "+ls.get(i));
			
		}
		while(true){
			input = scanner.nextLine();
			try{
				int num = Integer.parseInt(input);
				if(num >= 0 && num < ls.size()) {
					input1 = ls.get(num);
					break;
				}
				else {
					System.out.println("Invalid Input");
					continue;
				}
			}catch (NumberFormatException e) {
				e.printStackTrace();
				System.out.println("Invalid Input");
				continue;
			}
		}
		while(true){
			input = scanner.nextLine();
			try{
				int num = Integer.parseInt(input);
				if(num >= 0 && num < ls.size()) {
					input2 = ls.get(num);
					if(input2 != input1) {
						break;
					}
				}
				else {
					System.out.println("Invalid Input");
					continue;
				}
			}catch (NumberFormatException e) {
				e.printStackTrace();
				System.out.println("Invalid Input");
				continue;
			}
		}
		JoinManager m = new JoinManager(input1,input2);
		
		
	}
		
		
	
	//relation admin
	
	private void relationMang () {
		System.out.println("====================================================");
		System.out.println("                     DB Management");
		System.out.println("====================================================");
		List<String> ls = SQLRequester.Instance().getRelationList();
		System.out.println("관리할 릴레이션을 선택하세요.");
		for(int i=0;i<ls.size();++i) {
			
			System.out.println(i+" : "+ls.get(i));
		}
		while(true){
			input = scanner.nextLine();
			try{
				int num = Integer.parseInt(input);
				if(num >= 0 && num < ls.size()) {
					String input = ls.get(num);
					relationAdmin(SQLRequester.Instance().getRelationData(input));
					return;
				}
				else {
					System.out.println("Invalid Input");
					continue;
				}
			}catch (NumberFormatException e) {
				e.printStackTrace();
				System.out.println("Invalid Input");
				continue;
			}
			
		}
	}
	
	private void relationAdmin(RelationMetadata data) {
		RelationManager manager = new RelationManager(data);
		while(true){
			System.out.println("====================================================");
			System.out.println("                     Relation Admin ");
			System.out.println("====================================================");
			System.out.println("1. insert 2. delete 3. list all 4. search 5. quit");
			input = scanner.nextLine();
			
			switch(input) {
			case "1":
				manager.insert();
				break;
			case "2":
				manager.delete();
				break;
			case "3":
				manager.listAll();
				break;
			case "4":
				manager.printSearch();
				break; 
			case "5":
				return;
			default:
				System.out.println("Invalid Input");
				break;
			}
		}
		
	}
	
	private void relationCnD() {
		while(true){
			System.out.println("====================================================");
			System.out.println("                DB Creation & Deletion");
			System.out.println("====================================================");
			System.out.println("1. 릴레이션 추가 2. 릴레이션 제거 3. 종료");
			input = scanner.nextLine();
			switch(input) {
			case "1":
				create();
				break;
			case "2":
				delete();
				break;
			case "3":
				return;
			default:
				System.out.println("Invalid Input");
				break;
			}
		}
	}
	
	private void delete() {
		String filename;
		System.out.println("====================================================");
		System.out.println("                     DB Deletion");
		System.out.println("====================================================");
		System.out.println("Name of Database?");
		filename = scanner.nextLine();
		if(SQLRequester.Instance().isRelationExist(filename)) {
			File file = new File(filename+".bin");
			boolean deleted = file.delete();
	        if (!deleted) {
	            System.out.println("ERROR : fail to delete file.");
	            return;
	        } 
	        SQLRequester.Instance().dropReation(filename);
		}
		else {
			System.out.println("ERROR : no such relation exists.");
			return;
		}
	}
	
	private void create() {
		String filename;
		System.out.println("====================================================");
		System.out.println("                     DB Creation");
		System.out.println("====================================================");
		System.out.println("Name of Database?");
		filename = scanner.nextLine();
		
		if(SQLRequester.Instance().isRelationExist(filename)) {
			System.out.println("ERROR : The name of db is already made");
			return;
		}
		RelationMetadata meta = new RelationMetadata(filename);
		int num;
		while(true) {
			System.out.println("Number of attribute?");
			input = scanner.nextLine();
			try {
				num = Integer.parseInt(input);
				
				break;
			}catch(NumberFormatException e) {
				e.printStackTrace();
				System.out.println("Try Again (integer format)");
			}
		}
		for(int i=0;i<num;++i) {
			System.out.println("Name of attribute?");
			input = scanner.nextLine();
			System.out.println("Length of attribute?");
			try {
				int number = Integer.parseInt(scanner.nextLine());
				meta.addAttribute(input, number);
				continue;
			}catch(NumberFormatException e) {
				e.printStackTrace();
				System.out.println("Try Again (integer format)");
			}
		}
		SQLRequester.Instance().insertMeta(meta);
		BlockIOrequester io = new BlockIOrequester(filename);
		io.init();
	}
	
	
	
	
}
