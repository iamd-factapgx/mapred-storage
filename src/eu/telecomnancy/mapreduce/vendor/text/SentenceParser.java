package eu.telecomnancy.mapreduce.vendor.text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SentenceParser {
	
	private String sentence;
	private Map<Character, List<String>> words;
	
	final private char[] punctuations	= new char[]{',', ';', '?', '!', ':'};
	
	public SentenceParser(String sentence) {
		this.sentence = sentence;
	}
	
	public Map<Character, List<String>> getWords() {
		words				= new HashMap<Character, List<String>>();
		char[] chars		= sentence.toCharArray();
		List<String> parts	= new ArrayList<String>();
		String buffer		= "";
		
		for (int i=0; i < chars.length; i++) {
			if (punc(chars[i])) {
				parts.add(clean(buffer));
				buffer = "";
			} else {
				buffer += chars[i];
			}
		}
		parts.add(clean(buffer));
		
		for (String part : parts) {
			String[] split = part.split(" ");
			
			for (int i=0; i < split.length; i++) {
				for (int j=0; j < split.length-i; j++) {
					String sequence = "";
					for (int k=0; k < (i+1); k++) {
						sequence += split[j + k] + " ";
					}
					
					sequence = sequence.trim();
					
					if (sequence.isEmpty()) {
						continue;
					}
					char start =sequence.charAt(0);
					
					if (!words.containsKey(start)) {
						words.put(start, new ArrayList<String>());
					}
					if (!words.get(start).contains(sequence)) {
						words.get(start).add(sequence);
					}
				}
			}
		}
		
		return words;
	}

	private boolean punc(char c) {
		for (int j=0; j < punctuations.length; j++) {
			if (c == punctuations[j]) {
				return true;
			}
		}
		return false;
	}
	
	private String clean(String buffer) {
		String regex	= "[^a-zA-Z0-9]+";
		buffer			= buffer.trim();
		buffer			= buffer.replaceAll(regex + "$", "");
		buffer			= buffer.replaceAll("^" + regex, "");
		buffer			= buffer.replaceAll(regex + " ", " ");
		buffer			= buffer.replaceAll(" " + regex, " ");
		return buffer.toLowerCase();
	}
}
