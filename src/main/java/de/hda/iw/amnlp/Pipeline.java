package de.hda.iw.amnlp;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription;

import java.io.File;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.jcas.JCas;

import cc.mallet.topics.ParallelTopicModel;
import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import de.tudarmstadt.ukp.dkpro.core.mallet.topicmodel.MalletTopicModelEstimator;
import de.tudarmstadt.ukp.dkpro.core.mallet.topicmodel.MalletTopicModelInferencer;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;
import de.tudarmstadt.ukp.dkpro.core.stopwordremover.StopWordRemover;

/**
 * Pipeline with a reader, topic model estimator and inferencer, and writer.
 */
public class Pipeline {

	/** Storage location for topic model file */
	private static final File MODEL_FILE = new File("target/mallet/model");
	/** number of topics to estimate for the topic model */
	private static final int N_TOPICS = 10;
	/** number of iterations during model estimation */
	private static final int N_ITERATIONS = 50;

	/**
	 * Run pipelines
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// start
		Date start = new Date();
		System.out.println(start.toString() + " Start");

		// TODO put topic model estimation in it's own class, as topic models do
		// not have to be generated with each run

		//////////////////////////////////
		// WARNING!!! DO NOT RUN (FOR NOW)
		//////////////////////////////////
		//estimateTopicModels(start);

		// show topic models in console
		showTopicModels();
		//ArrayList bla = new ArrayList();
		//bla.remove(index)
		//createArffFile(start);

		// end
		Date end = new Date();
		System.out.println(end.toString() + " Ende");
	}

	/**
	 * Create file reader
	 * 
	 * @return
	 * @throws Exception
	 */
	private static CollectionReaderDescription getReader() throws Exception {

		// FIXME atm the corpus is split beforehand. write a csv reader

		// read files from disk
		CollectionReaderDescription imdbReader = createReaderDescription(TextReader.class,
				TextReader.PARAM_SOURCE_LOCATION, "input/imdb/*", TextReader.PARAM_LANGUAGE, "en");
		return imdbReader;
	}

	/**
	 * Estimate the topic models
	 * 
	 * @param start
	 * @throws Exception
	 */
	private static void estimateTopicModels(Date start) throws Exception {

		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMdd.HHmmss");
		String startDate = sdfDate.format(start);

		File file = new File("target/mallet/model." + startDate);

		// set up pipeline
		AnalysisEngineDescription pipeline = createEngineDescription(
				// segment files
				createEngineDescription(OpenNlpSegmenter.class),
				// remove stop words
				createEngineDescription(StopWordRemover.class, StopWordRemover.PARAM_MODEL_LOCATION,
						"[en]input/stop-word-list.txt"),
				// create tokens
				createEngineDescription(OpenNlpPosTagger.class),
				// estimate topic models
				createEngineDescription(MalletTopicModelEstimator.class,
						MalletTopicModelEstimator.PARAM_TARGET_LOCATION, file,
						MalletTopicModelEstimator.PARAM_N_ITERATIONS, N_ITERATIONS,
						MalletTopicModelEstimator.PARAM_N_TOPICS, N_TOPICS));

		// run pipeline for each document
		for (JCas jcas : SimplePipeline.iteratePipeline(getReader(), pipeline)) {
			
		}
		showTopicModels();
	}

	public static void showTopicModels() throws Exception {
		// print topic models for corpus
		//TODO make dynamic
		ParallelTopicModel model = ParallelTopicModel.read(MODEL_FILE);
		model.printTopWords(System.out, 10, true);
	}
	
	/**
	 * Create the arff file
	 * 
	 * @param start
	 * @throws Exception
	 */
	private static void createArffFile(Date start) throws Exception {
		int count = 0;

		ArffWriter arff = new ArffWriter();

		// set date format for file naming
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMdd.HHmmss");
		String startDate = sdfDate.format(start);

		// set up pipeline
		AnalysisEngineDescription pipeline = createEngineDescription(
				// segment files
				createEngineDescription(OpenNlpSegmenter.class),
				// create tokens
				createEngineDescription(OpenNlpPosTagger.class),
				// infer topic models
				// TODO find out how to use the date extracted here...
				createEngineDescription(MalletTopicModelInferencer.class,
						MalletTopicModelInferencer.PARAM_MODEL_LOCATION, MODEL_FILE));

		// limit iteration (optional)
		int max = 1000;

		// run pipeline for each document
		for (JCas jcas : SimplePipeline.iteratePipeline(getReader(), pipeline)) {
			Date time = new Date();

			Statistics stats = new Statistics(jcas);
			System.out.println(time.toString() + " file count:" + count++ + " token count: " + stats.getTokenCount());

			// only include non-empty documents 
			if (stats.getTokenCount() > 0) {
				// text is still a TSV ...
				String text = jcas.getCas().getDocumentText();
				Reader in = new java.io.StringReader(text);
				System.out.println(text);
				// ... parse it here, to retrieve the authorId ...
				CSVParser parser = new CSVParser(in, CSVFormat.TDF.withQuote(null).withHeader("reviewId", "userId",
						"itemId", "rating", "title", "content"));
				for (CSVRecord csvRecord : parser) {
					// .. and write data to arff file
					arff.addData(stats, Integer.parseInt(csvRecord.get("reviewId")), Integer.parseInt(csvRecord.get("userId")));
				}
				// garbage collection
				parser.close();
			}
			// stop if max count was reached
			if (max > 0 && count > max)
				break;
		}
		// write arff file
		arff.write("imdb62." + startDate);
	}
}
