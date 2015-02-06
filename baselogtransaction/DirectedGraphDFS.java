package baselogtransaction;

import java.util.ArrayList;

public class DirectedGraphDFS {

	// 邻接表中表对应的链表的顶点
	private class ENode {
		int ivex; // 该边所指向的顶点的位置
		ENode nextEdge; // 指向下一条弧的指针


	}

	// 邻接表中表的顶点
	private class VNode {
		String data; // 顶点信息
		ENode firstEdge; // 指向第一条依附该顶点的弧
	};

	private VNode[] mVexs; // 顶点数组

	/*
	 * 创建图(用已提供的矩阵)
	 * 
	 * 参数说明： vexs -- 顶点数组 edges -- 边数组
	 */
	public DirectedGraphDFS(String[] vexs, String[][] edges) {

		// 初始化"顶点数"和"边数"
		int vlen = vexs.length;
		int elen = edges.length;


		// 初始化"顶点"
		mVexs = new VNode[vlen];
		for (int i = 0; i < vlen; i++) {
			mVexs[i] = new VNode();
			mVexs[i].data = vexs[i];
			mVexs[i].firstEdge = null;
		}

		// 初始化"边"
		for (int i = 0; i < elen; i++) {
			// 读取边的起始顶点和结束顶点，在顶点数组中的位置
			int p1 = getPosition(edges[i][0]);
			int p2 = getPosition(edges[i][1]);

			// 初始化node1
			ENode node1 = new ENode();
			node1.ivex = p2;
			// 将node1链接到"p1所在链表的末尾"
			if (mVexs[p1].firstEdge == null)
				mVexs[p1].firstEdge = node1;
			else
				linkLast(mVexs[p1].firstEdge, node1);
		}
	}

	/*
	 * 将node节点链接到list的最后
	 */
	private void linkLast(ENode list, ENode node) {
		ENode p = list;

		while (p.nextEdge != null)
			p = p.nextEdge;
		p.nextEdge = node;
	}

	/*
	 * 返回ch位置
	 */
	private int getPosition(String ch) {
		for (int i = 0; i < mVexs.length; i++){
			if (mVexs[i].data.equals(ch)) { // 顶点数组中的位置
				return i;
			}
		}
		return -1;
	}

	// //////////////////////////////////////////////
	/*
	 * 深度优先搜索遍历图
	 */
	ArrayList<String> pathStack = new ArrayList<String>(); // 栈
	// 用来存储事务 数组，每一项都是一个事务。
	ArrayList<String> TransactionPathList = new ArrayList<String>();

	public ArrayList<String> DFS() {
	
		boolean[] visited = new boolean[mVexs.length]; // 顶点访问标记
		// 初始化所有顶点都没有被访问
		for (int i = 0; i < mVexs.length; i++)
			visited[i] = false;
			
		for (int i = 0; i < mVexs.length; i++) {
			//
			if (!visited[i])
				DFS(i, visited);
		}
		
		return TransactionPathList;

	}

	private void DFS(int i, boolean[] visited) {
		ENode node;
		visited[i] = true;
		pathStack.add(mVexs[i].data); // 入栈
		node = mVexs[i].firstEdge; // 链表的第一个
//
		if (node == null ) {
			String path = new String();
			for (int j = 0; j < pathStack.size(); j++) {
				path = path + pathStack.get(j) + "~";
			}
			TransactionPathList.add(path);
			
		} else {
			for (; node != null; node = node.nextEdge) {
			
				if (!visited[node.ivex]) { 
					DFS(node.ivex, visited);
				}
				
				
			}
		}
		
		pathStack.remove(pathStack.size() - 1); // 最后一个弹出栈
		
	}
}
