package de.hda.iw.amnlp;

import static org.apache.uima.fit.util.JCasUtil.select;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

import org.apache.uima.jcas.JCas;

import de.tudarmstadt.ukp.dkpro.core.api.frequency.util.FrequencyDistribution;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;

/**
 * Klasse für statistische Auswertungungen eines Dokuments
 */
public class Statistics {
	private JCas jcas;
	private Integer sentenceCount;
	private Integer paragraphCount;
	private Integer tokenCount;
	private Long adjectiveCount;
	private Long adverbCount;
	private Long verbCount;
	private Long nounCount;
	private FrequencyDistribution<String> posFreq;
	private Long numberCount;
	private Long foreignWordCount;
	private Long symbolCount;
	private Long interjectionCount;

	/**
	 * Konstruktor 
	 * @param jcas
	 * @throws Exception
	 */
	public Statistics(JCas jcas) throws Exception {
		this.jcas = jcas;
	}

	/**
	 * Gibt die Werte in der Konsole aus.
	 */
	public void print() {
		System.out.println("Anzahl Tokens (gesamt): " + getTokenCount());
		
		System.out.println("Anzahl Sätze: " + getSentenceCount());
		System.out.println("Durchschnittliche Tokens pro Satz: " + getAvgTokensPerSentence());
		System.out.println("Anzahl Absätze: " + getSentenceCount());
		System.out.println("Durchschnittliche Sätze pro Absatz: " + getAvgSentencesPerParagraph());

		System.out.println("Anzahl POS (gesamt): " + getPosFreq().getB());
		System.out.println("POS (Nomen): " + getNounCount());
		System.out.println("Anteil Nomen: " + getNounRate() + "%");
		System.out.println("POS (Verben): " + getVerbCount());
		System.out.println("Anteil Verben: " + getVerbRate() + "%");
		System.out.println("POS (Adverben): " + getAdverbCount());
		System.out.println("Anteil Adverben: " + getAdverbRate() + "%");
		System.out.println("POS (Adjektive): " + getAdjectiveCount());
		System.out.println("Anteil Adjektive: " + getAdjectiveRate() + "%");
		System.out.println("POS (Number): " + getNumberCount());
		System.out.println("Anteil Number: " + getNumberRate() + "%");
		System.out.println("POS (Symbol): " + getSymbolCount());
		System.out.println("Anteil Symbol: " + getSymbolRate() + "%");
		System.out.println("POS (Foreign Word): " + getForeignWordCount());
		System.out.println("Anteil Foreign Word: " + getForeignWordRate()+ "%");
		System.out.println("POS (Interjection): " + getInterjectionCount());
		System.out.println("Anteil Interjection: " + getInterjectionRate() + "%");
}


	/**
	 * Gibt eine Kommazahl auf zwei Nachkommastellen gerundet zurück.
	 * 
	 * @param number
	 * @return
	 */
	private String getRate(Double number) {
		Locale locale = new Locale("en", "UK");
		String pattern = "###.##";

		DecimalFormat decimalFormat = (DecimalFormat) NumberFormat.getNumberInstance(locale);
		decimalFormat.applyPattern(pattern);

		return decimalFormat.format(number);
	}

	/**
	 * Gibt die Frequency Distribution für Lemma zurück.
	 * 
	 * @return FrequencyDistribution<String>
	 */
	public FrequencyDistribution<String> getLemmaDistribution() {
		FrequencyDistribution<String> freq = new FrequencyDistribution<String>();
		for (Token token : select(this.jcas, Token.class)) {
			freq.inc(token.getLemma().getValue());
		}
		return freq;
	}

	/**
	 * Gibt die Anzahl an Sätzen im CAS zurück
	 * 
	 * @return Integer
	 */
	public Integer getSentenceCount() {
		if (this.sentenceCount == null)
			this.sentenceCount = select(this.jcas, Sentence.class).size();
		return this.sentenceCount;
	}

	/**
	 * Gibt die Anzahl an Tokens im CAS zurück
	 * 
	 * @return Integer
	 */
	public Integer getTokenCount() {
		if (this.tokenCount == null)
			this.tokenCount = select(this.jcas, Token.class).size();
		return this.tokenCount;
	}

	/**
	 * Gibt an, wieviel Tokens durchschnittlich pro Satz verwendet werden.
	 * 
	 * @return
	 */
	public String getAvgTokensPerSentence() {
		Double tokenpersentence = (double) getSentenceCount() / (double) getTokenCount() * 100;
		return getRate(tokenpersentence);
	}

	/**
	 * Gibt die Anzahl an Absätzen zurück.
	 * 
	 * @return
	 */
	public Integer getParagraphCount() {
		if (this.paragraphCount == null)
			this.paragraphCount = select(this.jcas, Paragraph.class).size();
		return this.paragraphCount;
	}

	/**
	 * Gibt an, wieviel Tokens durchschnittlich pro Satz verwendet werden.
	 * 
	 * @return
	 */
	public String getAvgSentencesPerParagraph() {
		Double sentenceperparagraph = (double) getSentenceCount() / (double) getParagraphCount() * 100;
		return getRate(sentenceperparagraph);
	}

	/**
	 * Returns the count of the Lemmata in the CAS
	 * 
	 * @return Integer
	 */
	public Integer getLemmaCount() {
		return (int) getLemmaDistribution().getB();
	}

	/**
	 * Berechnung des Verhältnisses von Lemma zu allen POS
	 * 
	 * @return String
	 */
	public String getLemmaRate() {
		Double lemmaRate = (double) getLemmaCount() / (double) getTokenCount() * 100;
		return getRate(lemmaRate);
	}

	/**
	 * Gibt die Frequency Distribution für POS zurück.
	 * 
	 * @return FrequencyDistribution<String>
	 */
	public FrequencyDistribution<String> getPosFreq() {
		if (this.posFreq == null) {
			this.posFreq = new FrequencyDistribution<String>();
			for (Token token : select(this.jcas, Token.class)) {
				this.posFreq.inc(token.getPos().getPosValue());
			}
		}
		return this.posFreq;
	}

	/**
	 * Zählt die Nomen im CAS.
	 * 
	 * @return Long
	 */
	public Long getNounCount() {
		if (this.nounCount == null) {
			this.nounCount = getPosFreq().getCount("NN") + getPosFreq().getCount("NNS") 
					+ getPosFreq().getCount("NNP") + getPosFreq().getCount("NNPS");
		}

		return this.nounCount;
	}

	/**
	 * Berechnung des Verhältnisses von Nomen zu allen POS
	 * 
	 * @return String
	 */
	public String getNounRate() {
		Double nounRate = (double) getNounCount() / (double) getTokenCount() * 100;
		return getRate(nounRate);
	}

	/**
	 * Zählt die Verben im Cas.
	 * 
	 * @return Long
	 */
	public Long getVerbCount() {
		if (this.verbCount == null)
			this.verbCount = getPosFreq().getCount("VB") + getPosFreq().getCount("VBD") 
				+ getPosFreq().getCount("VBG") + getPosFreq().getCount("VBN") 
				+ getPosFreq().getCount("VBP") + getPosFreq().getCount("VBZ");
		return this.verbCount;
	}

	/**
	 * Berechnung des Verhältnisses von Verben zu allen POS
	 * 
	 * @return String
	 */
	public String getVerbRate() {
		Double verbRate = (double) getVerbCount() / (double) getTokenCount() * 100;
		return getRate(verbRate);
	}

	/**
	 * Zählt die Adverben im Cas.
	 * 
	 * @return Long
	 */
	public Long getAdverbCount() {
		if (this.adverbCount == null)
			this.adverbCount = this.getPosFreq().getCount("RB") + getPosFreq().getCount("RBR")
					+ getPosFreq().getCount("RBS");
		return this.adverbCount;
	}

	/**
	 * Berechnung des Verhältnisses von Adverbien zu allen POS
	 * 
	 * @return String
	 */
	public String getAdverbRate() {
		Double adverbRate = (double) getAdverbCount() / (double) getTokenCount() * 100;
		return getRate(adverbRate);
	}

	/**
	 * Zählt die Advjektive im Cas.
	 * 
	 * @return Long
	 */
	public Long getAdjectiveCount() {
		if (this.adjectiveCount == null)
			this.adjectiveCount = getPosFreq().getCount("JJ") + getPosFreq().getCount("JJR")
					+ getPosFreq().getCount("JJS");
		return this.adjectiveCount;
	}

	/**
	 * Berechnung des Verhältnisses von Adjectiven zu allen POS
	 * 
	 * @return String
	 */
	public String getAdjectiveRate() {
		Double adjectiveRate = (double) getAdjectiveCount() / (double) getTokenCount() * 100;
		return getRate(adjectiveRate);
	}

	/**
	 * Zählt die Kardinalzahlen im Cas.
	 * 
	 * @return Long
	 */
	public Long getNumberCount() {
		if (this.numberCount == null)
			this.numberCount = getPosFreq().getCount("CD");
		return this.numberCount;
	}

	/**
	 * Berechnung des Verhältnisses von Zahlen zu allen POS
	 * 
	 * @return String
	 */
	public String getNumberRate() {
		Double numberRate = (double) getNumberCount() / (double) getTokenCount() * 100;
		return getRate(numberRate);
	}

	/**
	 * Zählt die Symbole im Cas.
	 * 
	 * @return Long
	 */
	public Long getSymbolCount() {
		if (this.symbolCount == null) {
			this.symbolCount = getPosFreq().getCount("SYM");
		}

		return this.symbolCount;
	}

	/**
	 * Berechnung des Verhältnisses von Symbolen zu allen POS
	 * 
	 * @return String
	 */
	public String getSymbolRate() {
		Double symbolRate = (double) getSymbolCount() / (double) getTokenCount() * 100;
		return getRate(symbolRate);
	}

	/**
	 * Zählt die Fremdwörter im Cas.
	 * 
	 * @return Long
	 */
	public Long getForeignWordCount() {
		if (this.foreignWordCount == null) {
			this.foreignWordCount = getPosFreq().getCount("FW");
		}

		return this.foreignWordCount;
	}

	/**
	 * Berechnung des Verhältnisses von Fremdwörtern zu allen POS
	 * 
	 * @return String
	 */
	public String getForeignWordRate() {
		Double foreignWordRate = (double) getForeignWordCount() / (double) getTokenCount() * 100;
		return getRate(foreignWordRate);
	}

	/**
	 * Zählt die Interpunktionszeichen im Cas.
	 * 
	 * @return Long
	 */
	public Long getInterjectionCount() {
		if (this.interjectionCount == null) {
			this.interjectionCount = getPosFreq().getCount("UH");
		}

		return this.interjectionCount;
	}

	/**
	 * Berechnung des Verhältnisses von Interpunktionszeichen zu allen POS
	 * 
	 * @return String
	 */
	public String getInterjectionRate() {
		Double interjectionRate = (double) getInterjectionCount() / (double) getTokenCount() * 100;
		return getRate(interjectionRate);
	}

	/**
	 * Gibt die Anzahl an NamedEntities zurück.
	 * 
	 * @return
	 */
	public Integer getNamedEntityCount() {
		return (int) getNamedEntitiyDistribution().getB();
	}

	/**
	 * Gibt die Frequency Distribution für NamedEntities zurück.
	 * 
	 * @return FrequencyDistribution<String>
	 */
	public FrequencyDistribution<String> getNamedEntitiyDistribution() {
		FrequencyDistribution<String> freq = new FrequencyDistribution<String>();
		for (NamedEntity ne : select(this.jcas, NamedEntity.class)) {
			freq.inc(ne.getValue());
		}
		return freq;
	}

	/**
	 * Berechnung des Verhältnisses von NamedEntites zu Lemmata
	 * 
	 * @return String
	 */
	public String getNamedEntityRate() {
		Double nerRate = (double) getNamedEntityCount() / (double) getLemmaCount() * 100;
		return getRate(nerRate);
	}
}
