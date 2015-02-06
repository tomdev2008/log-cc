package basetool;

import baselog.ClassifyNodes;
import baselog.ClickPath;
import baselog.Keywords;
import baselog.Links;
import basepath.ClassifyLandPage;
import basepath.ClassifyLeavePage;
import basepath.ClassifySourcePage;
import basepath.CountryCount;
import basepath.JumpCount;
import basepath.SessionCount;

public class RunAllJob {

	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();

		// 正则表 regex,ID
		String[][] regexTable = ReadFromMongo.regexFromMongo("regexDB",
				"level3");

		// hdfs输入
		String input = "hdfs://master1:9000/jlog/";
		// hdfs输出-输入
		String ClickPathOut = "hdfs://master1:9000/alog/ClickPath";

		// 输出到mongo
		String nodes = "mongodb://localhost:27017/jiaodian.nodes";
		String keywords = "mongodb://localhost:27017/jiaodian.keywords";
		String links = "mongodb://localhost:27017/jiaodian.links";

		String session = "mongodb://localhost:27017/jiaodian.session";
		String country = "mongodb://localhost:27017/jiaodian.country";
		String jump = "mongodb://localhost:27017/jiaodian.jump";

		String source = "mongodb://localhost:27017/jiaodian.source";
		String land = "mongodb://localhost:27017/jiaodian.land";
		String leave = "mongodb://localhost:27017/jiaodian.leave";

		// log >>
		int exitCodeClickPath = ClickPath.run(input, ClickPathOut);
		System.out.println("exitCodeClickPath" + ":\t" + exitCodeClickPath);

		int exitCodeKeywords = Keywords.run(input, keywords);
		System.out.println("exitCodeKeywords" + ":\t" + exitCodeKeywords);

		int exitCodeLinks = Links.run(input, links);
		System.out.println("exitCodeLinks" + ":\t" + exitCodeLinks);

		int exitCodeClassifyNodes = ClassifyNodes.run(input, nodes, regexTable);
		System.out.println("exitCodeClassifyNodes" + ":\t"
				+ exitCodeClassifyNodes);

		// job状态初始化
		int exitCodeSessionCount = 1;
		int exitCodeCountryCount = 1;
		int exitCodeJumpCount = 1;

		int exitCodeClassifySourcePage = 1;
		int exitCodeClassifyLandPage = 1;
		int exitCodeClassifyLeavePage = 1;

		// ClickPath完成后
		if (exitCodeClickPath == 0) {

			exitCodeSessionCount = SessionCount.run(ClickPathOut, session);
			System.out.println("exitCodeSessionCount" + ":\t"
					+ exitCodeSessionCount);

			exitCodeCountryCount = CountryCount.run(ClickPathOut, country);
			System.out.println("exitCodeCountryCount" + ":\t"
					+ exitCodeCountryCount);

			exitCodeJumpCount = JumpCount.run(ClickPathOut, jump);
			System.out.println("exitCodeJumpCount" + ":\t" + exitCodeJumpCount);

			exitCodeClassifySourcePage = ClassifySourcePage.run(ClickPathOut,
					source);
			System.out.println("exitCodeClassifySourcePage" + ":\t"
					+ exitCodeClassifySourcePage);

			exitCodeClassifyLandPage = ClassifyLandPage.run(ClickPathOut, land,
					regexTable);
			System.out.println("exitCodeClassifyLandPage" + ":\t"
					+ exitCodeClassifyLandPage);

			exitCodeClassifyLeavePage = ClassifyLeavePage.run(ClickPathOut,
					leave, regexTable);
			System.out.println("exitCodeClassifyLeavePage" + ":\t"
					+ exitCodeClassifyLeavePage);

		}

		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");

	}
}
