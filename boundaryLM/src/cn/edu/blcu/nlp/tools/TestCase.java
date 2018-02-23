package cn.edu.blcu.nlp.tools;

public class TestCase {
	private final String SEP_STRING="▲";
	private final String CIRCULAR_STRING="●";
	
	//private final char SEP_CHAR='▲'; 
	private final char CIRCULAR_CHAR='●';
	public static void main(String[] args) {
		String str1="▲并举办●中国比赛●";
		String lmFlag="left";
		System.out.println(new TestCase().circularReplace(str1, lmFlag, str1.length()));
		
	}
	
	private String circularReplace(String ngram,String lmFlag,int wordsNum){
		if(lmFlag.equalsIgnoreCase("left")){
			if(ngram.charAt(wordsNum-1)==CIRCULAR_CHAR){
				ngram=ngram.substring(0,wordsNum-1).replaceAll(CIRCULAR_STRING, SEP_STRING);
				ngram=ngram+CIRCULAR_CHAR;
			}else{
				ngram = ngram.replaceAll(CIRCULAR_STRING, SEP_STRING);
			}
			
		}else if(lmFlag.equalsIgnoreCase("right")){
			if(ngram.charAt(0)==CIRCULAR_CHAR){
				ngram=ngram.substring(1).replaceAll(CIRCULAR_STRING, SEP_STRING);
				ngram=CIRCULAR_CHAR+ngram;
			}else{
				ngram = ngram.replaceAll(CIRCULAR_STRING, SEP_STRING);
			}
			
		}
		return ngram;
	}
}
