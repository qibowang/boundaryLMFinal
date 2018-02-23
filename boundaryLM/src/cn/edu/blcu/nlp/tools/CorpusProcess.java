package cn.edu.blcu.nlp.tools;

public class CorpusProcess {
	private static String processLine(String line) {
		String posPattern = "( )?/[a-zA-Z]{1,5}( )?";
		String numberRegrex = "\\d+[.,]?\\d*";
		String numSign = "■";
		line = line.replaceAll(posPattern, "");
		line = line.replaceAll(numberRegrex, numSign);
		line = noneHZRep(line);
		line = line.replaceAll("(▲( ▲)*)+", "▲");
		line = line.replaceAll("(■( ■)*)+", "■");
		line = line.replaceAll("(●( ●)*)+", "●");
		return line;
	}

	private static String noneHZRep(String line) {
		StringBuffer sb = new StringBuffer();
		char numSign = '■';
		char triangleSign = '▲';
		char circularSign='●';
		// numSign
		
		char[] cArr = line.toCharArray();
		for (char ch : cArr) {
			// if('\u4e00' <= ch <= '\u9fff')
			if (ch >= '\u4e00' && ch <= '\u9fff')
				sb.append(ch);
			else if (ch == numSign)
				sb.append(ch);
			else if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))
				sb.append(ch);
			else if (ch == ' ')
				sb.append(' ');
			
			else if (ch == ','||ch=='.'||ch=='?'||ch=='!'||ch==';'||ch==':'||ch=='?'||ch=='，'||ch=='。'||ch=='！'||ch=='？'||ch=='、'||ch=='；'||ch=='：')
				sb.append(triangleSign);
			else
				sb.append(circularSign);
		}
		return sb.toString();
	}
	public static void main(String[] args) {
		String line="哈士奇/nr 大战/n 藏獒/n ——/w 不得不/d 说/v ，/w 哈士奇/nr 是/v 输/v 了/u 气势/n ，/w 却/d 赢/v 了/u 表情/n ./w ./w ./w （/w via/n 段玮/nr ）/w";
		line=processLine(line);
		System.out.println(line);
		line="唤起/v 公众/n 羞耻/n 意识/n 打破/v “/w 家具/n 抄袭/v 完整/a 链条/n ”/w";
		System.out.println(processLine(line));
		line="！/w 圣/Ag 蜜/Ag 莱/Ng 雅/Ag 即将/d 拍摄/v 阿/j sa/n 全新/b 优/Ag 肌/Ng 面/n 膜/n 广告/n ，/w 并/c 举办/v “/w 幸运/a 星/n ”/w 抽奖/vn 活动/vn ，/w 您/r 想来/v 拍片/v 现场/s 与/p 阿/j sa/n 零/m 距离/n 吗/y ？/w 您/r 想/v 获得/v twins/n 签名/vn CD/n 、/w 签名/v 照/v 吗/y ？/w 您/r 想/v 拥有/v 让/v 阿/j sa/n 爱不释手/i 的/u 优/Ag 肌/Ng 面/n 膜/n 吗/y ？/w 转发/v 此/r 条/q 微/Ag 博/Ag ，/w 即/d 有/v 机会/n 获得/v ，/w 大家/r 一起来/n 转发/v 吧/y ！/w 圣/Ag 蜜/Ag 莱/Ng 雅/Ag ，/w 以/p 水/n 养/v 白/a ，/w 每天/r 让/v 你/r 白/a 一点/m ！/w";
		System.out.println(processLine(line));
	}
}
