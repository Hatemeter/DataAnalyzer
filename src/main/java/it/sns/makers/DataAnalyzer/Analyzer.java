
package it.sns.makers.DataAnalyzer;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import eu.fbk.dh.kd.annotator.DigiKDAnnotations;
import eu.fbk.dh.kd.annotator.DigiKDResult;
import eu.fbk.dh.tint.runner.TintPipeline;
import org.apache.commons.io.FilenameUtils;
import org.gephi.appearance.plugin.palette.Palette;
import org.gephi.appearance.plugin.palette.PaletteManager;
import org.gephi.graph.api.Node;

import java.awt.*;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/* La libreria modularity di gephy è stata sostituita con una verisone sincrona
per evitare il bug della versione originale  : NonAsyncModularity Class
*/


public class Analyzer implements Runnable {

    private static  Logger LOGGER = null ;


    private Integer means = 0;
    private Integer stddevs = 0;


    private boolean topHash = false;
    private boolean hashNet = false;
    private boolean showTopRetweet = false;
    private boolean filterBadMentions = false;
    private boolean runTint = false;
    private boolean clusterCommunity = false;
    private boolean insertDB = false;
    private String lang = "en";

    private String mysqlUser = "";
    private String mysqlPassword = "";
    private String filePath = "";
    private Object pipeline = null;

    public Analyzer(String filePath, String lang, boolean topHash, boolean hashNet, boolean insertDB, boolean showTopRetweet, boolean filterBadMentions, boolean runTint, Integer means, Integer stddevs, Object pipeline) {
        Properties prop = new Properties();
        InputStream input = null;
        try {

            input = new FileInputStream("config.properties");

            // load a properties file
            prop.load(input);

            this.mysqlUser = prop.getProperty("mysqlUser");
            this.mysqlPassword = prop.getProperty("mysqlPassword");

        } catch (Exception e) {
            e.printStackTrace();
        }


        this.filterBadMentions = filterBadMentions;
        this.lang = lang;
        this.insertDB = insertDB;
        this.runTint = runTint;
        this.showTopRetweet = showTopRetweet;
        this.topHash = topHash;
        this.hashNet = hashNet;
        this.means = means;
        this.stddevs = stddevs;
        this.filePath = filePath;
        this.pipeline = pipeline;
    }


    @Override
    public void run() {

        Path mainFileName = Paths.get(filePath);
        String rootDir = FilenameUtils.removeExtension(mainFileName.getFileName().toString());
        this.LOGGER  = Logger.getLogger("Analyzer-"+rootDir);
        String twitterFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";

        SimpleDateFormat sf = new SimpleDateFormat(twitterFormat, Locale.ENGLISH);
        sf.setLenient(true);
        List<String> bad_mentions = new ArrayList<>();


        // *******  settings ********


        if (filterBadMentions) {
            System.out.println("filter bad mention");
            try {
                bad_mentions = Files.readAllLines(Paths.get("exclusion_list.txt"), Charset.forName("UTF-8"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        try {
            ConcurrentSkipListMap<Integer, StatsInTheDay> tweeterCounter = new ConcurrentSkipListMap<>();
            //setupWorkingDir

            System.out.println("------- setup working dir in " + rootDir + "-------");
            File theWorkingDir = new File(rootDir);
            if (!theWorkingDir.exists()) {
                theWorkingDir.mkdir();
            }
            String theWorkingDirPath = theWorkingDir.getAbsolutePath() + File.separator;

            // System.setProperty("user.dir", theWorkingDir.getAbsolutePath());

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            JsonReader reader = new JsonReader(new FileReader(filePath));
            reader.beginArray();
            long totalOfElements = 0;

            StatsInTheDay globalStats = new StatsInTheDay(Calendar.getInstance(), showTopRetweet, theWorkingDirPath);
            GraphForTags globalStatsForHashTag = new GraphForTags(theWorkingDirPath, means, stddevs);

            Map<String, Map<String, AtomicLong>> handle_hastagMapping = new HashMap<>();

            Writer outFileVerbs = null;
            Writer outFileNouns = null;
            Writer outFileAdj = null;
//                if (runTint) {
//                    outFileVerbs = new BufferedWriter(new OutputStreamWriter(
//                            new FileOutputStream(new File(theWorkingDirPath + "verbs.txt")), "UTF8"));
//
//                    outFileNouns = new BufferedWriter(new OutputStreamWriter(
//                            new FileOutputStream(new File(theWorkingDirPath + "nouns.txt")), "UTF8"));
//
//                    outFileAdj = new BufferedWriter(new OutputStreamWriter(
//                            new FileOutputStream(new File(theWorkingDirPath + "adjectives.txt")), "UTF8"));
//
//                }
            StringBuffer alltext = new StringBuffer();
            while (reader.hasNext()) {
                JsonObject tweet = gson.fromJson(reader, JsonObject.class);
                JsonArray mentions = tweet.getAsJsonObject("entities").getAsJsonArray("user_mentions");
                String screenName = "@" + tweet.getAsJsonObject("user").get("screen_name").getAsString().replace("\"", "");


                boolean skip = false;

                if (bad_mentions.contains(screenName)) {
                    skip = true;
                }
                for (int mn = 0; mn < mentions.size(); mn++) {
                    String user_mention = mentions.get(mn).getAsJsonObject().get("screen_name").getAsString() + " - " + mentions.get(mn).getAsJsonObject().get("name").getAsString();
                    if (bad_mentions.contains(screenName)) {
                        skip = true;
                        break;
                    }
                }


                if (tweet.get("lang").getAsString().equals(lang) && !skip) {
                    //screen name of author

                    Date creation_date = sf.parse(tweet.get("created_at").getAsString());
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(creation_date);
                    Integer tweeterKey = Integer.parseInt(String.valueOf(cal.get(Calendar.YEAR)) + String.valueOf(String.format("%02d", cal.get(Calendar.MONTH) + 1)) + String.valueOf(String.format("%02d", cal.get(Calendar.DAY_OF_MONTH))));


                    tweeterCounter.putIfAbsent(tweeterKey, new StatsInTheDay(cal, showTopRetweet, theWorkingDirPath));

                    //globalStats.addHandleNode(screenName);

                    handle_hastagMapping.putIfAbsent(screenName, new HashMap<>());

                    if (tweet.get("in_reply_to_status_id") != null) {
                        tweeterCounter.get(tweeterKey).directReply.incrementAndGet();
                        globalStats.directReply.incrementAndGet();
                    } else if (tweet.get("retweeted_status") == null) {
                        tweeterCounter.get(tweeterKey).tweetsCount.incrementAndGet();
                        Long id_tweet = (tweet.get("id").getAsLong());
                        globalStats.tweetsCount.incrementAndGet();
                        globalStats.nativeTweetList.add(id_tweet);
                        if (globalStats.nativeTweetStorage.containsKey(id_tweet)) {
                            System.out.println("already in");
                        }
                        globalStats.nativeTweetStorage.putIfAbsent(id_tweet, tweet);
                    } else {
                        tweeterCounter.get(tweeterKey).retweetsCount.incrementAndGet();
                        globalStats.retweetsCount.incrementAndGet();
                        // store the retweet count
                        Long id_retweet = (tweet.getAsJsonObject("retweeted_status")).get("id").getAsLong();
                        globalStats.incrementTopRetweet(id_retweet);

                    }


                    if (tweet.get("text").getAsString().contains("via @")) {
                        tweeterCounter.get(tweeterKey).viaCounter.incrementAndGet();
                        globalStats.viaCounter.incrementAndGet();

                    }


                    if (tweet.getAsJsonObject("extended_entities") != null) {
                        JsonArray media = tweet.getAsJsonObject("extended_entities").getAsJsonArray("media");
                        if (media != null) {
                            tweeterCounter.get(tweeterKey).withNativeMedia.incrementAndGet();
                            globalStats.withNativeMedia.incrementAndGet();
                            for (int md = 0; md < media.size(); md++) {
                                tweeterCounter.get(tweeterKey).incrementForMediaType(media.get(md).getAsJsonObject().get("type").getAsString());
                            }
                        }
                    }


                    // hashtag counter
                    JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
                    for (int ht = 0; ht < hashtags.size(); ht++) {
                        String tag = hashtags.get(ht).getAsJsonObject().get("text").getAsString();
                        tweeterCounter.get(tweeterKey).incrementForTag(tag);
                        globalStats.incrementForTag(tag);
                        //System.out.println(screenName + "\t" + "#" + tag.toLowerCase() );
                        handle_hastagMapping.get(screenName).putIfAbsent("#" + tag.toLowerCase(), new AtomicLong(0));
                        handle_hastagMapping.get(screenName).get("#" + tag.toLowerCase()).incrementAndGet();


                    }


                    if (hashtags.size() > 1) {
                        for (int x = 0; x < hashtags.size() - 1; x++) {
                            for (int j = x + 1; j < hashtags.size(); j++) {
                                String tag1 = "#" + hashtags.get(x).getAsJsonObject().get("text").getAsString().toLowerCase();
                                String tag2 = "#" + hashtags.get(j).getAsJsonObject().get("text").getAsString().toLowerCase();
                                if (!tag1.equals(tag2)) {
                                    globalStatsForHashTag.addEdge(tag1, tag2);
                                }
                            }
                        }

                    }


                    tweeterCounter.get(tweeterKey).overallHashtagStats.addValue(hashtags.size());
                    globalStats.overallHashtagStats.addValue(hashtags.size());


                    if (mentions.size() > 0) {
                        tweeterCounter.get(tweeterKey).howManyContainsMentions.incrementAndGet();
                        globalStats.howManyContainsMentions.incrementAndGet();
                    }


                    for (int mn = 0; mn < mentions.size(); mn++) {
                        String user_mention = mentions.get(mn).getAsJsonObject().get("screen_name").getAsString() + " - " + mentions.get(mn).getAsJsonObject().get("name").getAsString();
                        tweeterCounter.get(tweeterKey).incrementForMention(user_mention);
                        globalStats.incrementForMention(user_mention);
                    }


                    //// building edges according to the tweet class  (tweet / retweet / reply)
                    if (tweet.get("in_reply_to_status_id") != null) {
                        // is a reply
                        String replyScreenName = "@" + (tweet.get("in_reply_to_screen_name").getAsString().replace("\"", ""));
                        globalStats.addEdgeFormTo(screenName, replyScreenName);

                    } else if (tweet.get("retweeted_status") == null) {
                        // is a tweet

                        // loop over the mention and make link from author to mentions
                        for (int mn = 0; mn < mentions.size(); mn++) {
                            String user_mention = "@" + mentions.get(mn).getAsJsonObject().get("screen_name").getAsString().replace("\"", "");
                            globalStats.addEdgeFormTo(screenName, user_mention);
                        }

                        // if no mention make a self loop
                        if (mentions.size() == 0) {
                            globalStats.addEdgeFormTo(screenName, screenName);
                        }

                    } else {
                        // is a retweet
                        String originalTweetauthor = "@" + tweet.getAsJsonObject("retweeted_status").getAsJsonObject("user").get("screen_name").getAsString().replace("\"", "");

                        globalStats.addEdgeFormTo(screenName, originalTweetauthor);
                        // create edeges form retwitter form all the mentions of the retweet
                        for (int mn = 0; mn < mentions.size(); mn++) {
                            String user_mention = "@" + mentions.get(mn).getAsJsonObject().get("screen_name").getAsString().replace("\"", "");
                            if (!user_mention.equals(originalTweetauthor)) {
                                globalStats.addEdgeFormTo(screenName, user_mention);
                            }
                        }

                    }


                    tweeterCounter.get(tweeterKey).overallMentionStats.addValue(mentions.size());
                    globalStats.overallMentionStats.addValue(mentions.size());


                    totalOfElements++;


                    String text = tweet.get("text").getAsString();
                    if (tweet.get("retweeted_status") != null) {
                        text = tweet.getAsJsonObject("retweeted_status").get("text").getAsString();
                        alltext.append(text + "\n");
                    } else {
                        // System.out.println(tweet.toString());
                        //text = tweet.getAsJsonObject("retweeted_status").get("text").getAsString();
                        alltext.append(tweet.get("text").getAsString() + "\n");
                    }

                    // running tint

//                        if (runTint) {
//                            if (text.trim().length() > 0) {
//                                Annotation annotation = pipeline.runRaw(text);
//                                List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
//
//                                for (CoreMap sentence : sentences) {
//                                    List<CoreLabel> tokens = sentence.get(CoreAnnotations.TokensAnnotation.class);
//                                    for (CoreLabel c : tokens) {
//
//                                        if (((c.get(CoreAnnotations.PartOfSpeechAnnotation.class).equals("S") || c.get(CoreAnnotations.PartOfSpeechAnnotation.class).equals("A") || c.get(CoreAnnotations.PartOfSpeechAnnotation.class).startsWith("V")) && (!c.get(CoreAnnotations.LemmaAnnotation.class).toLowerCase().startsWith("htt") && !c.get(CoreAnnotations.LemmaAnnotation.class).startsWith("#") && !c.get(CoreAnnotations.LemmaAnnotation.class).startsWith("@")))) {
//                                            if (!c.get(CoreAnnotations.LemmaAnnotation.class).equals("essere") && !c.get(CoreAnnotations.LemmaAnnotation.class).equals("avere")) {
//                                                if (c.get(CoreAnnotations.PartOfSpeechAnnotation.class).startsWith("V")) {
//                                                    outFileVerbs.append(screenName + "\t" + c.get(CoreAnnotations.LemmaAnnotation.class).toLowerCase() + "\n");
//                                                } else if (c.get(CoreAnnotations.PartOfSpeechAnnotation.class).startsWith("S")) {
//                                                    outFileNouns.append(screenName + "\t" + c.get(CoreAnnotations.LemmaAnnotation.class).toLowerCase() + "\n");
//                                                } else if (c.get(CoreAnnotations.PartOfSpeechAnnotation.class).startsWith("A")) {
//                                                    outFileAdj.append(screenName + "\t" + c.get(CoreAnnotations.LemmaAnnotation.class).toLowerCase() + "\n");
//                                                }
//
//                                            }
//                                        }
//
//                                    }
//                                }
//                            }
//
//                            //  outFileVerbs.flush();
//                            //  outFileNouns.flush();
//                            //  outFileAdj.flush();
//                        }

                } else {
                    // System.out.println("No it");
                    // System.out.println(tweet);
                }
            }


            // handle_hastagMapping contains all hashtag with freq used by an user
//                if (runTint) {
//                    outFileVerbs.close();
//                    outFileNouns.close();
//                    outFileAdj.close();
//                }


            Writer outFileStats = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(new File(theWorkingDirPath + "stats.txt")), "UTF8"));
            System.out.println("Overall");
            System.out.println("Date\tTweets\tRetweets\tReply\tTotMentions\tWithMedia\ttitle");
            System.out.println(globalStats.toString());


            JsonArray overall_stats = new JsonArray();

            outFileStats.write("Overall\n");
            outFileStats.write("Date\tTweets\tRetweets\tReply\tTotMentions\tWithMedia\ttitle\n");
            outFileStats.write(globalStats.toString() + "\n");
            overall_stats.add(globalStats.toString().split("\t")[1]);
            overall_stats.add(globalStats.toString().split("\t")[2]);
            overall_stats.add(globalStats.toString().split("\t")[3]);


                /*
                Insert previously processed top retweet

                try {
                    String snapShotName = (new File(theWorkingDirPath).getName());
                    //snapShotName = snapShotName.replace("_1_","_01_").replace("_2_","_02_").replace("_3_","_03_").replace("_4_","_04_").replace("_5_","_05_").replace("_6_","_06_").replace("_7_","_07_").replace("_8_","_08_").replace("_9_","_09_");
                    //snapShotName = snapShotName.replace("_1-","_01-").replace("_2-","_02-").replace("_3-","_03-").replace("_4-","_04-").replace("_5-","_05-").replace("_6-","_06-").replace("_7-","_07-").replace("_8-","_08-").replace("_9-","_09-");
                    Connection dbconn = null;
                    try {
                        // Class.forName("org.gjt.mm.mysql.Driver");
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        dbconn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/HateMeter?autoreconnect=true&allowMultiQueries=true&connectTimeout=0&socketTimeout=0&useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC", mysqlUser, mysqlPassword);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    PreparedStatement pstmt = dbconn.prepareStatement("REPLACE INTO " + lang + "_top_retweet VALUES (?,?)");
                    pstmt.setString(1, snapShotName);
                    pstmt.setString(2, globalStats.topRetweetArrayJson.toString());
                    pstmt.execute();
                    dbconn.close();
                } catch (Exception e) {
                }

                if (skipToNextFile) {
                    continue;
                }
                     */
            //globalStats.analyzeGraph();


            JsonObject hashtag_daily_stats = new JsonObject();

            hashtag_daily_stats.add("labels", new JsonArray());
            hashtag_daily_stats.add("datasets", new JsonArray());

            JsonObject tweet = new JsonObject();
            tweet.addProperty("label", "Tweets");
            tweet.addProperty("backgroundColor", "rgba(220, 220, 220, 0.5)");
            tweet.addProperty("borderColor", "rgba(220, 220, 220, 0.8)");
            tweet.addProperty("borderWidth", 1);
            tweet.addProperty("hoverBackgroundColor", "rgba(220, 220, 220, 0.8)");
            tweet.addProperty("hoverBorderColor", "rgba(220, 220, 220, 0.1)");
            tweet.add("data", new JsonArray());

            JsonObject retweet = new JsonObject();
            retweet.addProperty("label", "Re-Tweets");
            retweet.addProperty("backgroundColor", "rgba(151, 187, 205, 0.5)");
            retweet.addProperty("borderColor", "rgba(151, 187, 205, 0.8)");
            retweet.addProperty("borderWidth", 1);
            retweet.addProperty("hoverBackgroundColor", "rgba(151, 187, 205, 0.8)");
            retweet.addProperty("hoverBorderColor", "rgba(151, 187, 205, 0.8)");
            retweet.add("data", new JsonArray());

            JsonObject reply = new JsonObject();
            reply.addProperty("label", "Reply");
            reply.addProperty("backgroundColor", "rgba(255,99,132,0.5)");
            reply.addProperty("borderColor", "rgba(255,99,132,8)");
            reply.addProperty("borderWidth", 1);
            reply.addProperty("hoverBackgroundColor", "rgba(255,99,132,0.8)");
            reply.addProperty("hoverBorderColor", "rgba(255,99,132,1)");
            reply.add("data", new JsonArray());


            System.out.println("Date\tTweets\tRetweets\tReply\tTotMentions\tWithMedia\ttitle");
            outFileStats.write("Date\tTweets\tRetweets\tReply\tTotMentions\tWithMedia\ttitle" + "\n");
            for (Map.Entry<Integer, StatsInTheDay> entry : tweeterCounter.entrySet()) {
                System.out.println(entry.getValue().toString());
                String[] items = entry.getValue().toString().split("\t");
                hashtag_daily_stats.getAsJsonArray("labels").add(items[0]);

                tweet.get("data").getAsJsonArray().add(items[1]);
                retweet.get("data").getAsJsonArray().add(items[2]);
                reply.get("data").getAsJsonArray().add(items[3]);


                outFileStats.write(entry.getValue().toString() + "\n");
            }

            hashtag_daily_stats.get("datasets").getAsJsonArray().add(tweet);
            hashtag_daily_stats.get("datasets").getAsJsonArray().add(retweet);
            hashtag_daily_stats.get("datasets").getAsJsonArray().add(reply);


            outFileStats.close();


            //System.out.println(hashtag_daily_stats);


            // top hashtag
            if (topHash) {
                Writer outFile = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(new File(theWorkingDirPath + "top_hashtag.tsv")), "UTF8"));
                for (Map.Entry<String, Integer> entry : globalStats.getTopHashTags(999999999).entrySet()) {
                    outFile.write(entry.getKey() + "\t" + entry.getValue() + "\n");
                }
                outFile.close();

            }


            Map<String, Integer> handle_class_map = new HashMap<>();


            //System.out.println(handle_class_map.toString());
            //System.out.println(handle_hastagMapping.toString());


            ArrayList<String> filesToBeUsed = new ArrayList();
            filesToBeUsed.add("verbs.txt");
            filesToBeUsed.add("nouns.txt");
            filesToBeUsed.add("adjectives.txt");


            if (clusterCommunity) {

                Map<Integer, ArrayList<String>> classMentionMap = new HashMap<>();
                for (Node n : globalStats.getDirectedGraph().getNodes()) {
                    classMentionMap.putIfAbsent(Integer.parseInt(n.getAttribute("modularity_class").toString()), new ArrayList<>());
                    classMentionMap.get(Integer.parseInt(n.getAttribute("modularity_class").toString())).add(n.getLabel());
                    handle_class_map.putIfAbsent(n.getLabel(), Integer.parseInt(n.getAttribute("modularity_class").toString()));
                }

                System.out.println("Number of communities : " + classMentionMap.size());
                AtomicLong clusertId = new AtomicLong(1);
                classMentionMap
                        .entrySet()
                        .stream()
                        .sorted((left, right) ->
                                Integer.compare(right.getValue().size(), left.getValue().size()))
                        .limit(6)
                        .forEach((k) -> {
                            System.out.println("------------Cluster " + clusertId + " " + k.getValue().size() + "--------------");


                            //System.out.println(k.getKey() + " " + k.getValue().size());
                            for (String fname : filesToBeUsed) {
                                ArrayList<String> listofitems = new ArrayList();
                                try {
                                    Stream<String> lines = Files.lines(Paths.get(theWorkingDirPath, fname));
                                    lines.forEach(s -> {
                                        if (handle_class_map.get(s.split("\t")[0]).equals(k.getKey())) {
                                            listofitems.add(s.split("\t")[1]);
                                        }
                                    });
                                    lines.close();

                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                try {
                                    Writer outFileCluster = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(theWorkingDirPath + fname + "_" + clusertId + "_" + k.getValue().size() + ".txt")), "UTF8"));
                                    Map<String, Long> counts = listofitems.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
                                    counts.entrySet().stream()
                                            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                                            .forEach((it) -> {
                                                try {
                                                    outFileCluster.append(it.getKey() + "\t" + it.getValue() + "\n");
                                                } catch (Exception x) {
                                                    x.printStackTrace();
                                                }

                                            }); // or any other terminal method
                                    outFileCluster.close();

                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }


                            /// hashtag cluster....
                            Map<String, Integer> clusterHashtag_freq = new HashMap<>();
                            try {
                                Writer outFileClusterMention = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(theWorkingDirPath + "mentions" + "_" + clusertId + "_" + k.getValue().size() + ".txt")), "UTF8"));
                                for (String mention : classMentionMap.get(k.getKey())) {
                                    Map<String, AtomicLong> stringAtomicLongMap = handle_hastagMapping.get(mention);
                                    outFileClusterMention.write(mention + "\n");
                                    if (stringAtomicLongMap != null) {
                                        for (String key_hash : stringAtomicLongMap.keySet()) {
                                            clusterHashtag_freq.putIfAbsent(key_hash, 0);
                                            clusterHashtag_freq.put(key_hash, clusterHashtag_freq.get(key_hash) + stringAtomicLongMap.get(key_hash).intValue());
                                        }
                                    }
                                }
                                outFileClusterMention.close();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            try {
                                Writer outFileClusterTags = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(theWorkingDirPath + "tags" + "_" + clusertId + "_" + k.getValue().size() + ".txt")), "UTF8"));

                                clusterHashtag_freq.entrySet().stream()
                                        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                                        .forEach((it) -> {
                                            try {
                                                outFileClusterTags.append(it.getKey() + "\t" + it.getValue() + "\n");
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            }
                                        });

                                outFileClusterTags.close();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            ////
                            clusertId.incrementAndGet();
                        });
            }


            //Most Used Verbs
//                if (runTint) {
//                    try {
//                        Map<String, Integer> verb_mapping = new HashMap<>();
//                        Stream<String> lines = Files.lines(Paths.get(theWorkingDirPath, "verbs.txt"));
//                        lines.forEach(s -> {
//                            verb_mapping.putIfAbsent(s.split("\t")[1], 0);
//                            verb_mapping.put(s.split("\t")[1], verb_mapping.get(s.split("\t")[1]) + 1);
//                        });
//                        Writer outFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(theWorkingDirPath + "mostUsedVerbs.txt")), "UTF8"));
//                        verb_mapping.entrySet().stream()
//                                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
//                                .forEach((it) -> {
//                                    try {
//                                        outFile.write(it.getValue() + "\t" + it.getKey() + "\n");
//                                    } catch (IOException e) {
//                                        e.printStackTrace();
//                                    }
//                                });
//                        outFile.close();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
            globalStats.analyzeGraph();
            globalStats.exportGraph();

            if (hashNet) {

                globalStatsForHashTag.saveDataForHeatmap();
                //globalStatsForHashTag.analyzeGraph();
                globalStatsForHashTag.exportGraph();
            }
            reader.endArray();
            reader.close();


            Connection dbconn = null;
            try {
                // Class.forName("org.gjt.mm.mysql.Driver");
                Class.forName("com.mysql.cj.jdbc.Driver");
                dbconn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/HateMeter?autoreconnect=true&allowMultiQueries=true&connectTimeout=0&socketTimeout=0&useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC", mysqlUser, mysqlPassword);
            } catch (Exception e) {
                e.printStackTrace();
            }

            JsonArray keywords = new JsonArray();

            System.out.println("---------- Process text and extract keywords------------");

            if (runTint) {
                if (lang.equals("en")) {
                    Annotation annotation = new Annotation(alltext.toString().replaceAll("#[A-Za-z]+", "").replaceAll("@[A-Za-z]+", ""));
                    ((StanfordCoreNLP) pipeline).annotate(annotation);
                    System.out.println("Stanford like pipeline ended...");
                    List<DigiKDResult> r = annotation.get(DigiKDAnnotations.KeyphrasesAnnotation.class);
                    if (r.size() > 0) {
                        Palette palette = PaletteManager.getInstance().generatePalette(r.size());
                        r.forEach(digiKDResult -> {
                            Color c = palette.getColors()[r.indexOf(digiKDResult)];
                            JsonObject key = new JsonObject();
                            key.addProperty("name", digiKDResult.getKeyphrase());
                            key.addProperty("value", digiKDResult.getScore());
                            key.add("textStyle", new JsonObject());
                            key.get("textStyle").getAsJsonObject().add("normal", new JsonObject());
                            key.get("textStyle").getAsJsonObject().get("normal").getAsJsonObject().addProperty("color", "rgb(" + c.getRed() + "," + c.getGreen() + "," + c.getBlue() + ")");
                            keywords.add(key);
                        });
                    }

                    //System.out.println(keywords.toString());
                } else if (lang.equals("it")) {
                    Annotation annotation = ((TintPipeline) pipeline).runRaw(alltext.toString().replaceAll("#[A-Za-z]+", "").replaceAll("@[A-Za-z]+", ""));
                    System.out.println("Stanford like pipeline ended...");
                    List<DigiKDResult> r = annotation.get(DigiKDAnnotations.KeyphrasesAnnotation.class);
                    if (r.size() > 0) {
                        Palette palette = PaletteManager.getInstance().generatePalette(r.size());
                        r.forEach(digiKDResult -> {
                            Color c = palette.getColors()[r.indexOf(digiKDResult)];
                            JsonObject key = new JsonObject();
                            key.addProperty("name", digiKDResult.getKeyphrase());
                            key.addProperty("value", digiKDResult.getScore());
                            key.add("textStyle", new JsonObject());
                            key.get("textStyle").getAsJsonObject().add("normal", new JsonObject());
                            key.get("textStyle").getAsJsonObject().get("normal").getAsJsonObject().addProperty("color", "rgb(" + c.getRed() + "," + c.getGreen() + "," + c.getBlue() + ")");
                            keywords.add(key);
                        });
                    }
                    // System.out.println(keywords.toString());
                } else if (lang.equals("fr")) {

                    Annotation annotation = new Annotation(alltext.toString().replaceAll("#[A-Za-z]+", "").replaceAll("@[A-Za-z]+", ""));
                    ((StanfordCoreNLP) pipeline).annotate(annotation);
                    System.out.println("Stanford like pipeline ended...");
                    List<DigiKDResult> r = annotation.get(DigiKDAnnotations.KeyphrasesAnnotation.class);
                    if (r.size() > 0) {
                        Palette palette = PaletteManager.getInstance().generatePalette(r.size());
                        r.forEach(digiKDResult -> {
                            Color c = palette.getColors()[r.indexOf(digiKDResult)];
                            JsonObject key = new JsonObject();
                            key.addProperty("name", digiKDResult.getKeyphrase());
                            key.addProperty("value", digiKDResult.getScore());
                            key.add("textStyle", new JsonObject());
                            key.get("textStyle").getAsJsonObject().add("normal", new JsonObject());
                            key.get("textStyle").getAsJsonObject().get("normal").getAsJsonObject().addProperty("color", "rgb(" + c.getRed() + "," + c.getGreen() + "," + c.getBlue() + ")");
                            keywords.add(key);
                        });
                    }
                }

            }

            System.out.println("   done !!");
            // System.out.println(alltext);


            if (insertDB) {

                String snapShotName = (new File(theWorkingDirPath).getName());
                snapShotName = snapShotName.replace("_1_", "_01_").replace("_2_", "_02_").replace("_3_", "_03_").replace("_4_", "_04_").replace("_5_", "_05_").replace("_6_", "_06_").replace("_7_", "_07_").replace("_8_", "_08_").replace("_9_", "_09_");
                snapShotName = snapShotName.replace("_1-", "_01-").replace("_2-", "_02-").replace("_3-", "_03-").replace("_4-", "_04-").replace("_5-", "_05-").replace("_6-", "_06-").replace("_7-", "_07-").replace("_8-", "_08-").replace("_9-", "_09-");
                // System.out.println(snapShotName);
                try {
                    PreparedStatement pstmt = dbconn.prepareStatement("REPLACE INTO " + lang + "_stats VALUES (?,?,?)");
                    pstmt.setString(1, snapShotName);
                    pstmt.setString(2, hashtag_daily_stats.toString());
                    pstmt.setString(3, overall_stats.toString());
                    pstmt.execute();
                } catch (Exception e) {
                }
                try {
                    File file = new File(theWorkingDirPath + "/" + "hash_gexf.json");
                    String result = com.google.common.io.Files.toString(file, Charsets.UTF_8);

                    PreparedStatement pstmt = dbconn.prepareStatement("REPLACE INTO " + lang + "_hashtag_nets VALUES (?,?)");
                    pstmt.setString(1, snapShotName);
                    pstmt.setString(2, result);
                    pstmt.execute();
                } catch (Exception e) {
                }

                try {
                    File file = new File(theWorkingDirPath + "/" + "mentions_net.json");
                    String result = com.google.common.io.Files.toString(file, Charsets.UTF_8);

                    PreparedStatement pstmt = dbconn.prepareStatement("REPLACE INTO " + lang + "_mention_nets VALUES (?,?)");
                    pstmt.setString(1, snapShotName);
                    pstmt.setString(2, result);
                    pstmt.execute();
                } catch (Exception e) {
                }

                try {
                    PreparedStatement pstmt = dbconn.prepareStatement("REPLACE INTO " + lang + "_hashkeywords VALUES (?,?)");
                    pstmt.setString(1, snapShotName);
                    pstmt.setString(2, keywords.toString());
                    pstmt.execute();

                } catch (Exception e) {
                }

                try {
                    PreparedStatement pstmt = dbconn.prepareStatement("REPLACE INTO " + lang + "_top_retweet VALUES (?,?)");
                    pstmt.setString(1, snapShotName);
                    pstmt.setString(2, globalStats.topRetweetArrayJson.toString());
                    pstmt.execute();

                } catch (Exception e) {
                }


                try {
                    JsonObject mention_connections = new JsonObject();
                    mention_connections.add("labels", new JsonArray());
                    mention_connections.add("datasets", new JsonArray());
                    JsonObject mentionsData = new JsonObject();
                    mentionsData.addProperty("label", "Connections");
                    mentionsData.addProperty("backgroundColor", "rgba(151, 187, 205, 0.5)");
                    mentionsData.addProperty("borderColor", "rgba(151, 187, 205, 0.8)");
                    mentionsData.addProperty("borderWidth", 1);
                    mentionsData.addProperty("hoverBackgroundColor", "rgba(151, 187, 205, 0.8)");
                    mentionsData.addProperty("hoverBorderColor", "rgba(151, 187, 205, 0.8)");
                    mentionsData.add("data", new JsonArray());
                    globalStats.degreeMentionsStats.entrySet().stream().limit(15).forEach(stringIntegerEntry -> {
                        mention_connections.get("labels").getAsJsonArray().add(stringIntegerEntry.getKey());
                        mentionsData.get("data").getAsJsonArray().add(stringIntegerEntry.getValue());
                    });
                    mention_connections.get("datasets").getAsJsonArray().add(mentionsData);

                    PreparedStatement pstmt = dbconn.prepareStatement("REPLACE INTO " + lang + "_mention_stats VALUES (?,?)");
                    pstmt.setString(1, snapShotName);
                    pstmt.setString(2, mention_connections.toString());
                    pstmt.execute();
                } catch (Exception e) {
                }

                dbconn.close();
            }

            globalStatsForHashTag = null;
            globalStats = null;

            //   System.out.println(totalOfElements);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}






