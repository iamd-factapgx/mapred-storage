package eu.telecomnancy.mapreduce.vendor.text;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.Tokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.GrammaticalStructureFactory;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreebankLanguagePack;
import edu.stanford.nlp.trees.TypedDependency;
import edu.stanford.nlp.util.CoreMap;

public class StanfordParser {
	
	private static TreebankLanguagePack tlp;
	private static GrammaticalStructureFactory gsf;
	private static LexicalizedParser lp;
	private static StanfordCoreNLP pipeline;
	
	public StanfordParser() {
		lp			= LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");
		tlp			= new PennTreebankLanguagePack();
		gsf			= tlp.grammaticalStructureFactory();
		pipeline	= new StanfordCoreNLP();
	}
	
	/**
	 * Parse a sentence and return dependency graph
	 * @param text
	 * @return
	 */
	public List<String> parse(String text) {
		text = text.toLowerCase();
		
		TokenizerFactory<CoreLabel> tokenizerFactory	= PTBTokenizer.factory(new CoreLabelTokenFactory(), "");
		Tokenizer<CoreLabel> tok						= tokenizerFactory.getTokenizer(new StringReader(text));
		List<CoreLabel> rawWords						= tok.tokenize();
		Tree parse										= lp.apply(rawWords);
		
		GrammaticalStructure gs							= gsf.newGrammaticalStructure(parse);
		List<String> results = new ArrayList<String>();
		
		for (TypedDependency dependency : gs.typedDependenciesCCprocessed()) {
			results.add(dependency.toString());
		}
		
		return results;
	}
	
	/**
	 * Split text into sentences
	 * @param text
	 * @return
	 */
	public List<String> split(String text) {
		List<String> results = new ArrayList<String>();
		
		Annotation document = new Annotation(text);
		pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		
		for (CoreMap sentence: sentences) {
			results.add(sentence.toString());
		}
		
		return results;
	}
}
