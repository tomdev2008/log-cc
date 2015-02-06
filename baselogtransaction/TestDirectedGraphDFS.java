package baselogtransaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

public class TestDirectedGraphDFS {
	public static void main(String[] args) {

		// 初始数据:已经按时间排好序的 边表
		String[] strpath = { "a~b", "b~c", "c~d", "d~e", "d~f", "b~g", "g~h",
				"b~i","c~h","c~f"};  //,"f~a","c~a",

		// 存储边表
		String[][] edgeArray = new String[strpath.length][2];
		// 用来 暂存点表
		HashSet<String> vertexSet = new HashSet<String>();
		for (int i = 0; i < strpath.length; i++) {
			String[]  edgeSplit = ((String) strpath[i]).split("~");// referer,request
			// 直接构造边表
			edgeArray[i][0] =  edgeSplit[0]; // referer
			edgeArray[i][1] =  edgeSplit[1]; // request
			// 利用set构造点表
			 vertexSet.add( edgeSplit[0]);
			 vertexSet.add( edgeSplit[1]);
		}

		// 构造点表，保证起点，其他点的顺序无所谓
		// 顶点表
		String[] vertexArray = new String[vertexSet.size()];
		vertexSet.remove(edgeArray[0][0]); // 删除hashset中的起点
		vertexArray[0] = edgeArray[0][0];// 存储起点
		// 其他点随便存
		Iterator<String> ihs =  vertexSet.iterator();
		int j = 1;
		while (ihs.hasNext()) {
			vertexArray[j] = (String) ihs.next();
			j++;
		}

		// 调用DirectedGraphDFS
		// 利用顶点表和边表,创建图
		DirectedGraphDFS pG;
		pG = new DirectedGraphDFS(vertexArray, edgeArray);
		ArrayList<String> TransactionPathList = new ArrayList<String>();
		TransactionPathList = pG.DFS(); // 深度优先遍历
		// 输出事务
		for (int m = 0; m < TransactionPathList.size(); m++) {
			System.out.println(TransactionPathList.get(m));
		}
	}
}
