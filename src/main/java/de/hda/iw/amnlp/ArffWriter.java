package de.hda.iw.amnlp;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class ArffWriter {

	/**
	 * 
	 */
	private String data = "";

	/**
	 * 
	 * @param stats
	 * @param bool
	 */
	public void addData(Statistics stats, int documentId, int authorId) {
		this.data += 
				documentId + ","
				+ stats.getTokenCount() + "," 
				+ stats.getNounCount() + ","
				+ stats.getNounRate() + "," 
				+ stats.getAdjectiveCount() + "," 
				+ stats.getAdjectiveRate() + ","
				+ stats.getAdverbCount() + "," 
				+ stats.getAdverbRate() + "," 
				+ stats.getVerbCount() + ","
				+ stats.getVerbRate() + "," 
				+ stats.getNumberCount() + "," 
				+ stats.getNumberRate() + ","
				+ stats.getSymbolCount() + "," 
				+ stats.getSymbolRate() + "," 
				+ stats.getForeignWordCount() + ","
				+ stats.getForeignWordRate() + "," 
				+ stats.getInterjectionCount() + ","
				+ stats.getInterjectionRate() + "," 
				+ stats.getSentenceCount() + "," 
				+ stats.getAvgTokensPerSentence() + "," 
				+ authorId + "\r\n";
	}

	/**
	 * Schreibt die Daten im arff-Format
	 * 
	 * @param filename Dateiname
	 * 
	 * @throws IOException
	 * @return void
	 */
	public void write(String filename) throws IOException {
	    BufferedWriter writer = new BufferedWriter(new FileWriter("./output/" + filename + ".arff"));
		writer.write("@RELATION scientific\r\n");
		writer.write("\r\n");
		writer.write("@ATTRIBUTE documentId NUMERIC\r\n");
		writer.write("@ATTRIBUTE tokencount NUMERIC\r\n");
		writer.write("@ATTRIBUTE nouncount NUMERIC\r\n");
		writer.write("@ATTRIBUTE nounrate NUMERIC\r\n");
		writer.write("@ATTRIBUTE adjectivecount NUMERIC\r\n");
		writer.write("@ATTRIBUTE adjectiverate NUMERIC\r\n");
		writer.write("@ATTRIBUTE adverbcount NUMERIC\r\n");
		writer.write("@ATTRIBUTE adverbrate NUMERIC\r\n");
		writer.write("@ATTRIBUTE verbcount NUMERIC\r\n");
		writer.write("@ATTRIBUTE verbrate NUMERIC\r\n");
		writer.write("@ATTRIBUTE numbercount NUMERIC\r\n");
		writer.write("@ATTRIBUTE numberrate NUMERIC\r\n");
		writer.write("@ATTRIBUTE symbolcount NUMERIC\r\n");
		writer.write("@ATTRIBUTE symbolrate NUMERIC\r\n");
		writer.write("@ATTRIBUTE foreignwordcount NUMERIC\r\n");
		writer.write("@ATTRIBUTE foreignwordrate NUMERIC\r\n");
		writer.write("@ATTRIBUTE interjectioncount NUMERIC\r\n");
		writer.write("@ATTRIBUTE interjectionrate NUMERIC\r\n");
		writer.write("@ATTRIBUTE sentencecount NUMERIC\r\n");
		writer.write("@ATTRIBUTE tokenpersentence NUMERIC\r\n");
		writer.write("@ATTRIBUTE authorId NUMERIC\r\n");
		writer.write("\r\n");
		writer.write("@DATA\r\n");
		writer.write(this.data);
		writer.flush();
		writer.close();

	}
}
