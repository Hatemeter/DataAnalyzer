package it.sns.makers.DataAnalyzer;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import eu.fbk.dh.tint.runner.TintPipeline;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MainParallel {
    public static void main(String[] args) {

        Integer means = 0;
        Integer stddevs = 0;


        boolean topHash = false;
        boolean hashNet = false;
        boolean showTopRetweet = false;
        boolean filterBadMentions = false;
        boolean runTint = false;
        boolean clusterCommunity = false;
        boolean insertDB = false;
        String lang = "en";

        String treetaggerPath = "";
        String mysqlUser = "";
        String mysqlPassword = "";
        Properties prop = new Properties();
        InputStream input = null;
        try {

            input = new FileInputStream("config.properties");

            // load a properties file
            prop.load(input);

            treetaggerPath = prop.getProperty("treetaggerPath");
            mysqlUser = prop.getProperty("mysqlUser");
            mysqlPassword = prop.getProperty("mysqlPassword");

        } catch (Exception e) {
            e.printStackTrace();
        }

        Options options = new Options();

        options.addOption("l", "language", true, "language  (default en)");
        options.addOption("hc", "hashtag_net_cutoff", true, "cutoff value: use m for mean and s fot std deviation : es. m+s+s ( mean + (2*sdt_dev) )");

        options.addOption("t", "topHashTag", false, "extract topHashTag");
        options.addOption("hn", "hashtag_network", false, "generate hashtag network");
        options.addOption("tr", "top_retweet", false, "extract top retweet");
        options.addOption("fm", "filter_mention", false, "filter out mentions listed in exclusion_list.txt file");
        options.addOption("lp", "run_nlp", false, "run language analyzer");
        options.addOption("db", "insert_db", false, "insert into db");

        options.addOption("h", "help", false, "show help");
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(500);
        CommandLine cmd = null;
        try {
            CommandLineParser parser = new DefaultParser();
            cmd = parser.parse(options, args);

            topHash = cmd.hasOption("topHashTag");
            hashNet = cmd.hasOption("hashtag_network");

            insertDB = cmd.hasOption("insert_db");

            showTopRetweet = cmd.hasOption("top_retweet");
            filterBadMentions = cmd.hasOption("filter_mention");
            runTint = cmd.hasOption("run_nlp");

            if (!cmd.hasOption("language")) {
                System.out.println("Specify language");
                formatter.printHelp("SMTP", options);

                System.exit(0);
            } else {
                lang = cmd.getOptionValue("language");
            }

            if (cmd.hasOption("help")) {
                formatter.printHelp("SMTP", options);
                System.exit(0);
            }


            if (cmd.hasOption("hashtag_net_cutoff")) {
                String cutoff = cmd.getOptionValue("hashtag_net_cutoff");
                means = StringUtils.countMatches(cutoff, "m");
                stddevs = StringUtils.countMatches(cutoff, "s");

            } else {
                lang = cmd.getOptionValue("language");
            }


        } catch (Exception e) {
            formatter.printHelp("SMTP", options);

        }



        Object pipeline = null;

        if (runTint) {
            try {
                if (lang.equals("it")) {
                    pipeline = new TintPipeline();
                    ((TintPipeline) pipeline).loadDefaultProperties();
                    ((TintPipeline) pipeline).setProperty("annotators", "ita_toksent, pos,ita_morpho, ita_lemma,keyphrase");
                    ((TintPipeline) pipeline).setProperty("nthread", "6");
                    ((TintPipeline) pipeline).setProperty("customAnnotatorClass.keyphrase", "eu.fbk.dh.kd.annotator.DigiKDAnnotator");
                    ((TintPipeline) pipeline).setProperty("keyphrase.numberOfConcepts", "20");
                    ((TintPipeline) pipeline).setProperty("keyphrase.local_frequency_threshold", "2");
                    ((TintPipeline) pipeline).setProperty("keyphrase.language", "ITALIAN");


                    // ((TintPipeline) pipeline).setProperty("ita_toksent.model", "/Users/giovannimoretti/Dropbox/DropBoxesCodes/SPMT/token-settings.xml");
                    ((TintPipeline) pipeline).load();
                } else if (lang.equals("en")) {
                    Properties props = new Properties();
                    props.setProperty("ssplit.newlineIsSentenceBreak", "always");
                    props.setProperty("annotators", "tokenize,ssplit,pos,lemma,keyphrase");
                    props.setProperty("ner.useSUTime", "false");
                    props.setProperty("ner.applyFineGrained", "false");
                    props.setProperty("customAnnotatorClass.keyphrase", "eu.fbk.dh.kd.annotator.DigiKDAnnotator");
                    props.setProperty("keyphrase.numberOfConcepts", "20");
                    props.setProperty("keyphrase.local_frequency_threshold", "2");

                    pipeline = new StanfordCoreNLP(props);


                } else if (lang.equals("fr")) {
                    Properties props = new Properties();

                    props.setProperty("ssplit.newlineIsSentenceBreak", "always");
                    props.setProperty("annotators", "tokenize,ssplit,pos,lemma,keyphrase");
                    props.setProperty("ner.useSUTime", "false");
                    props.setProperty("ner.applyFineGrained", "false");
                    props.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/french/french.tagger");
                    props.setProperty("tokenize.language", "fr");
                    props.setProperty("'pipelineLanguage'", "fr");
                    props.setProperty("customAnnotatorClass.keyphrase", "eu.fbk.dh.kd.annotator.DigiKDAnnotator");
                    props.setProperty("keyphrase.numberOfConcepts", "20");
                    props.setProperty("keyphrase.local_frequency_threshold", "2");
                    props.setProperty("keyphrase.language", "CUSTOM");
                    props.setProperty("keyphrase.languageName", "FRENCH");


                    pipeline = new StanfordCoreNLP(props);
                }


            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        List<String> filePaths = cmd.getArgList();
        ExecutorService executor = Executors.newFixedThreadPool(1);
        for (String filePath : filePaths) {
            Analyzer a = new Analyzer(filePath,lang,topHash,hashNet,insertDB,showTopRetweet,filterBadMentions,runTint,means,stddevs,pipeline);
            executor.execute(a);

        }

        executor.shutdown();
        try {
            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        }catch (Exception e){
            e.printStackTrace();
        }
        executor.shutdownNow();

    }
}
