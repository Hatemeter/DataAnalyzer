package it.sns.makers.DataAnalyzer;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.gephi.appearance.api.*;
import org.gephi.appearance.plugin.PartitionElementColorTransformer;
import org.gephi.appearance.plugin.palette.Palette;
import org.gephi.appearance.plugin.palette.PaletteManager;
import org.gephi.graph.api.*;
import org.gephi.io.exporter.api.ExportController;
import org.gephi.layout.plugin.forceAtlas2.ForceAtlas2;
import org.gephi.layout.plugin.forceAtlas2.ForceAtlas2Builder;
import org.gephi.project.api.ProjectController;
import org.gephi.project.api.Workspace;
import org.gephi.statistics.plugin.Degree;
import org.gephi.statistics.plugin.GraphDistance;
import org.gephi.statistics.plugin.Modularity;
import org.gephi.utils.progress.ProgressTicket;
import org.gephi.utils.progress.ProgressTicketProvider;
import org.openide.util.Lookup;
import uk.ac.ox.oii.jsonexporter.JSONExporter;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class StatsInTheDay {


    /* graph */
    private ProjectController pc;
    private GraphModel graphModel;
    private DirectedGraph directedGraph;
    private Workspace workspace;
    private AppearanceController appearanceController;
    private AppearanceModel appearanceModel;


    private Calendar thisRefDate;
    private boolean showTopRetweeted = false;
    private String theWorkingDirPath;


    public AtomicLong howManyContainsMentions = new AtomicLong(0);
    public AtomicLong howManyContainsMedia = new AtomicLong(0);

    public AtomicLong tweetsCount = new AtomicLong(0);
    public AtomicLong retweetsCount = new AtomicLong(0);
    public AtomicLong directReply = new AtomicLong(0);
    public AtomicLong viaCounter = new AtomicLong(0);
    public AtomicLong withNativeMedia = new AtomicLong(0);
    public AtomicLong withNonNativeMedia = new AtomicLong(0);

    public AtomicLong total_numer_of_hashtag = new AtomicLong(0);

    public Set<Long> nativeTweetList = new HashSet<>();
    public Map<Long, JsonObject> nativeTweetStorage = new HashMap<>();


    public Map<String, Integer> topConcurrentHashTags = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
    public Map<String, Integer> topConcurrentUserMentions = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
    public Map<String, Integer> topLanguages = new HashMap<>();

    public Map<String, Integer> mediaType = new HashMap<>();

    public SummaryStatistics overallHashtagStats = new SummaryStatistics();
    public SummaryStatistics overallMentionStats = new SummaryStatistics();

    public Map<Long, Integer> topRetweeted = new TreeMap<Long, Integer>();


    public Map<String, Integer> degreeMentionsStats = new HashMap<>();


    public JsonArray topRetweetArrayJson = new JsonArray();


    public double getDailyAverageOfTag() {
        return total_numer_of_hashtag.intValue() / (double) (this.tweetsCount.intValue() + this.retweetsCount.intValue());
    }

    public double getDailyFrequencyOfTag(String tag, int numberOfMention) {
        return this.getTopHashTags(numberOfMention).get(tag) / (double) (this.tweetsCount.intValue() + this.retweetsCount.intValue());
    }

    public double getDailyFrequencyOfMention(String mention, int numberOfMention) {
        return this.getTopUserMentions(numberOfMention).get(mention) / (double) (this.tweetsCount.intValue() + this.retweetsCount.intValue());
    }


    public StatsInTheDay(Calendar ref, boolean showTopRetweeted, String theWorkingDirPath) {
        this.theWorkingDirPath = theWorkingDirPath;
        this.thisRefDate = ref;
        this.pc = Lookup.getDefault().lookup(ProjectController.class);
        pc.newProject();
        this.workspace = pc.getCurrentWorkspace();
        this.graphModel =  Lookup.getDefault().lookup(GraphController.class).getGraphModel(this.workspace);
        this.appearanceController = Lookup.getDefault().lookup(AppearanceController.class);
        this.appearanceModel = appearanceController.getModel();
        this.directedGraph = graphModel.getDirectedGraph();
        this.showTopRetweeted = showTopRetweeted;
    }

    public void incrementForTag(String tag) {
        topConcurrentHashTags.putIfAbsent(tag, 0);
        topConcurrentHashTags.put(tag, topConcurrentHashTags.get(tag) + 1);
    }

    public void incrementForMention(String mention) {
        topConcurrentUserMentions.putIfAbsent(mention, 0);
        topConcurrentUserMentions.put(mention, topConcurrentUserMentions.get(mention) + 1);
    }

    public void incrementForLang(String lang) {
        topLanguages.putIfAbsent(lang, 0);
        topLanguages.put(lang, topLanguages.get(lang) + 1);
    }

    public void incrementForMediaType(String type) {
        mediaType.putIfAbsent(type, 0);
        mediaType.put(type, mediaType.get(type) + 1);
    }


    public void incrementTopRetweet(Long id) {
        topRetweeted.putIfAbsent(id, 0);
        topRetweeted.put(id, topRetweeted.get(id) + 1);
    }

    public void addHandleNode(String screename) {
        Node aNode = graphModel.factory().newNode(screename);
        aNode.setLabel(screename);
        aNode.setSize(10);
        try {
            directedGraph.addNode(aNode);
        } catch (Exception e) {
            //System.err.println("Dup node for scree name " + screename);
        }
    }

    public DirectedGraph getDirectedGraph(){
        return this.directedGraph;
    }

    public void addEdgeFormTo(String source, String target) {
        addHandleNode(source);
        addHandleNode(target);

        if (directedGraph.getEdge(directedGraph.getNode(source), directedGraph.getNode(target)) != null) {
            directedGraph.getEdge(directedGraph.getNode(source), directedGraph.getNode(target)).setWeight(directedGraph.getEdge(directedGraph.getNode(source), directedGraph.getNode(target)).getWeight() + 1.0);
        } else {
            Edge anEdge = graphModel.factory().newEdge(directedGraph.getNode(source), directedGraph.getNode(target), true);
            anEdge.setWeight(1.0);
            directedGraph.addEdge(anEdge);
        }


    }


    public LinkedHashMap<String, Integer> getTopHashTags(int n) {
        LinkedHashMap<String, Integer> out = new LinkedHashMap<>();
        topConcurrentHashTags.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(n)
                .forEach((k) -> {
                    out.put(k.getKey(), k.getValue());
                });
        return out;
    }


    public LinkedHashMap<String, Integer> getTopUserMentions(int n) {
        LinkedHashMap<String, Integer> out = new LinkedHashMap<>();
        topConcurrentUserMentions.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(n)
                .forEach((k) -> {
                    out.put(k.getKey(), k.getValue());
                });
        return out;
    }


    public LinkedHashMap<String, Integer> getTopMediaType(int n) {
        LinkedHashMap<String, Integer> out = new LinkedHashMap<>();

        mediaType.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(n)
                .forEach((k) -> {
                    out.put(k.getKey(), k.getValue());
                });
        return out;
    }

    public LinkedHashMap<String, Integer> getTopLanguages() {
        LinkedHashMap<String, Integer> out = new LinkedHashMap<>();

        topLanguages.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .forEach((k) -> {
                    out.put(k.getKey(), k.getValue());
                });
        return out;
    }

    public void analyzeGraph() {


        System.out.println("Graph nodes:" + directedGraph.getNodeCount());
        System.out.println("Edges:" + directedGraph.getEdgeCount());
        // Math operator

        //Degree
        Degree degree = new Degree();
        degree.execute(directedGraph.getModel());


        //distance to make centrality of nodes
        GraphDistance gd = new GraphDistance();
        gd.execute(directedGraph.getModel());




        //Eingenvector to make eingen of nodes
//        EigenvectorCentrality ev = new EigenvectorCentrality();
//        ev.execute(directedGraph.getModel());


        System.out.printf("Desity: %f\n", ((2 * (directedGraph.getEdgeCount() - directedGraph.getNodeCount() + 1)) / (float) (directedGraph.getNodeCount() * (directedGraph.getNodeCount() - 3) + 2)));
        // System.out.println("Modularity:" + String.valueOf(mod.getModularity()).replace(".", ","));
        System.out.println("K mean degree:" + String.valueOf(degree.getAverageDegree()).replace(".", ","));


        // remove comment to sys out to print in out betweeness

        /*
        System.out.println("\nInDegree");
        inDegreeNodeMap.entrySet().stream()
                .sorted(Map.Entry.<Node, Integer>comparingByValue().reversed())

                .filter((k) -> k.getValue() >= mean_indegree + sd_indegree)
                .forEach((k) -> {
                    System.out.println(k.getKey().getLabel() + "\t" + k.getKey().getAttribute("indegree") + "\t" + k.getKey().getAttribute("closnesscentrality") + "\t" + k.getKey().getAttribute("betweenesscentrality") + "\t" + k.getKey().getAttribute("eigencentrality"));
                });

        System.out.println("\noutDegree");

        outDegreeNodeMap.entrySet().stream()
                .sorted(Map.Entry.<Node, Integer>comparingByValue().reversed())

                .filter((k) -> k.getValue() >= mean_outdegree + sd_outdegree)
                .forEach((k) -> {
                    System.out.println(k.getKey().getLabel() + "\t" + k.getKey().getAttribute("outdegree") + "\t" + k.getKey().getAttribute("closnesscentrality") + "\t" + k.getKey().getAttribute("betweenesscentrality") + "\t" + k.getKey().getAttribute("eigencentrality"));
                });

        System.out.println("\nbetweeness");
        closnesscentralityNodeMap.entrySet().stream()
                .sorted(Map.Entry.<Node, Double>comparingByValue().reversed())

                .filter((k) -> k.getValue() >= mean_betweeness + sd_betweeness)
                .forEach((k) -> {
                    System.out.println(k.getKey().getLabel() + "\t" + k.getKey().getAttribute("degree") + "\t" + k.getKey().getAttribute("closnesscentrality") + "\t" + k.getKey().getAttribute("betweenesscentrality") + "\t" + k.getKey().getAttribute("eigencentrality"));
                });

        eingenvectorNodeMap.entrySet().stream()
                .sorted(Map.Entry.<Node, Double>comparingByValue().reversed())

                .filter((k) -> k.getValue() >= mean_betweeness + sd_betweeness)
                .forEach((k) -> {
                    System.out.println(k.getKey().getLabel() + "\t" + k.getKey().getAttribute("degree") + "\t" + k.getKey().getAttribute("closnesscentrality") + "\t" + k.getKey().getAttribute("betweenesscentrality") + "\t" + k.getKey().getAttribute("eigencentrality"));
                });


        betweenesscentralityNodeMap.entrySet().stream()
                .sorted(Map.Entry.<Node, Double>comparingByValue().reversed())

                .filter((k) -> k.getValue() >= mean_betweeness + sd_betweeness)
                .forEach((k) -> {
                    System.out.println(k.getKey().getLabel() + "\t" + k.getKey().getAttribute("degree") + "\t" + k.getKey().getAttribute("closnesscentrality") + "\t" + k.getKey().getAttribute("betweenesscentrality") + "\t" + k.getKey().getAttribute("eigencentrality"));
                });
        */

        Map<String,Integer> degreeList = new HashMap<>();

        directedGraph.getNodes().forEach(node -> {
            degreeList.put(node.getLabel(),(Integer)node.getAttribute("degree"));
        });

        this.degreeMentionsStats = degreeList.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));






    }


    public double normalizeValue(Double val, Double min, Double max) {
        return (val - min) / (max - min);
    }


    public DirectedGraph recoursiveCutOff(DirectedGraph dg){


        if (dg.getNodes().toArray().length > 1500){
            System.out.println("Apply a cutoff...");
            Column degreeColumn = this.graphModel.getNodeTable().getColumn("degree");
            Function func = appearanceModel.getNodeFunction(dg, degreeColumn, PartitionElementColorTransformer.class);
            Partition partition = ((PartitionFunction) func).getPartition();

            Set<Node> toBeRemovedNode = new HashSet<>();
            System.out.println("Number of partitions:"+partition.getSortedValues().toArray().length);
            dg.getNodes().forEach(node -> {
                if ((Integer) node.getAttribute("degree") == partition.getSortedValues().toArray()[0]) {
                    toBeRemovedNode.add(node);
                }
            });

            toBeRemovedNode.forEach(node -> {
                dg.removeNode(node);
            });
            System.out.println(" --After cutoff -> Graph Nodes:" + dg.getNodeCount() + "  Edges:" + dg.getEdgeCount());
            recoursiveCutOff(dg);

        }
        return dg;
    }


    public void exportGraph() {
        System.out.println("\n----------- Mentions Graph -----------");
        System.out.println("Graph nodes:" + directedGraph.getNodeCount() + " - " + "Edges:" + directedGraph.getEdgeCount());



        directedGraph=  recoursiveCutOff(directedGraph);


        /*if (directedGraph.getNodes().toArray().length > 1500){
            Column degreeColumn = this.graphModel.getNodeTable().getColumn("degree");
            Function func = appearanceModel.getNodeFunction(directedGraph, degreeColumn, PartitionElementColorTransformer.class);
            Partition partition = ((PartitionFunction) func).getPartition();

            Set<Node> toBeRemovedNode = new HashSet<>();

            directedGraph.getNodes().forEach(node -> {
                if ((Integer) node.getAttribute("degree") == partition.getSortedValues().toArray()[0]) {
                    toBeRemovedNode.add(node);
                }
            });

            toBeRemovedNode.forEach(node -> {
                directedGraph.removeNode(node);
            });

            System.out.println("Big Net --After cutoff -> Graph Nodes:" + directedGraph.getNodeCount() + "  Edges:" + directedGraph.getEdgeCount());

        }
        */
        System.out.println("Big Net --After cutoff -> Graph Nodes:" + directedGraph.getNodeCount() + "  Edges:" + directedGraph.getEdgeCount());

        //modularity
        NonAsyncModularity mod = new NonAsyncModularity();
        mod.setResolution(1.0);
        mod.setRandom(false);
        mod.setUseWeight(true);
        mod.execute(directedGraph.getModel());



        Column modColumn = this.graphModel.getNodeTable().getColumn(Modularity.MODULARITY_CLASS);
        Function func = appearanceModel.getNodeFunction(directedGraph, modColumn, PartitionElementColorTransformer.class);

        if (directedGraph.getNodeCount() > 0) {
            Partition partition = ((PartitionFunction) func).getPartition();
            Palette palette = PaletteManager.getInstance().generatePalette(partition.size());


            directedGraph.getNodes().forEach(node -> {
                node.setColor(palette.getColors()[(Integer) (node.getAttribute("modularity_class"))]);
            });

            directedGraph.getEdges().forEach(edge -> {
                edge.setColor(edge.getSource().getColor());
            });


            ForceAtlas2 f2 = new ForceAtlas2Builder().buildLayout();
            f2.setGraphModel(graphModel);
            f2.setScalingRatio(600.0);
            f2.setGravity(1.0);
            f2.setStrongGravityMode(true);
            f2.setThreadsCount(7);
            f2.setJitterTolerance(1.0);
            f2.setLinLogMode(false);
            f2.setAdjustSizes(false);

            f2.initAlgo();
            for (int i = 0; i < 2000 && f2.canAlgo(); i++) {
                f2.goAlgo();
            }
            f2.endAlgo();


            ExportController ec = Lookup.getDefault().lookup(ExportController.class);

            ProgressTicketProvider progressProvider = Lookup.getDefault().lookup(ProgressTicketProvider.class);
            ProgressTicket ticket = null;
            if (progressProvider != null) {
                ticket = progressProvider.createTicket("Task name", null);
            }


            JSONExporter jex = new JSONExporter();
            jex.setWorkspace(this.workspace);
            jex.setExportVisible(true);
            jex.setProgressTicket(ticket);

//Export only visible graph
            // GraphExporter exporter = (GraphExporter) ec.getExporter("gexf");     //Get GEXF exporter
            // exporter.setExportVisible(true);  //Only exports the visible (filtered) graph
            // exporter.setWorkspace(this.workspace);


            try {
                ec.exportFile(new File(this.theWorkingDirPath + "mentions_net.json"), jex);
                // ec.exportFile(new File(this.theWorkingDirPath + "mentions_net.gexf"), exporter);
            } catch (IOException ex) {
                ex.printStackTrace();
                return;
            }


            System.out.println("Mention Graph Exported");
        }else{
            System.out.println("Graph to small !!");
        }
        System.out.println("-------------------------------------");

    }


    @Override
    public String toString() {
        JsonParser parser = new JsonParser();
        String data = String.valueOf(String.format("%02d", thisRefDate.get(Calendar.DAY_OF_MONTH))) + "/" + String.valueOf(String.format("%02d", thisRefDate.get(Calendar.MONTH) + 1)) + "/" + String.valueOf(thisRefDate.get(Calendar.YEAR));
        StringBuffer out = new StringBuffer();
        out.append("\nDate: " + data + "\n");
        out.append("Tweets: " + tweetsCount + "\n");
        out.append("ReTweets: " + retweetsCount + "\n");
        out.append("Reply: " + directReply + "\n");
        out.append("Via: " + viaCounter + "\n");
        out.append("Media:" + (mediaType.size() > 0 ? mediaType : " not collected") + "\n");
        out.append("\nConcurrent tags:\n");


        if (this.showTopRetweeted) {
            StringBuffer outp = new StringBuffer();
            this.topRetweetArrayJson = new JsonArray();
            topRetweeted.entrySet().stream()
                    .sorted(Map.Entry.<Long, Integer>comparingByValue().reversed())
                    .limit(10)
                    .forEach((k) -> {
                        if (nativeTweetStorage.keySet().contains(k.getKey())) {
                            JsonObject tweet = nativeTweetStorage.get(k.getKey());
                            // uncomment to print top tweets
                            JsonArray media = null;
                            if (tweet.getAsJsonObject("extended_entities") != null) {
                                if (tweet.getAsJsonObject("extended_entities").getAsJsonArray("media") != null) {
                                    media = tweet.getAsJsonObject("extended_entities").getAsJsonArray("media");
                                }
                            }

                            //stampa text + user + media

//                            System.out.println(tweet.toString());

                            JsonObject o = parser.parse(tweet.toString()).getAsJsonObject();
                            outp.append(tweet.get("text") + "\t" + tweet.getAsJsonObject("user").get("name") + "\t" + tweet.getAsJsonObject("user").get("description") + "\thttps://twitter.com/" + o.get("user").getAsJsonObject().get("screen_name").getAsString() + "/status/" + k.getKey() + "\t" + k.getValue() + "\tmedia:\t" + (media != null ? "YES" : "NO") + "\n");


                            JsonObject topTweet = new JsonObject();
                            topTweet.addProperty("text",tweet.get("text").getAsString());
                            topTweet.addProperty("link","https://twitter.com/" + o.get("user").getAsJsonObject().get("screen_name").getAsString() + "/status/" + k.getKey());
                            topTweet.addProperty("date", tweet.get("created_at").getAsString() );
                            topTweet.addProperty("retweet", k.getValue());
                            this.topRetweetArrayJson.add(topTweet);
                        }
                    });
            if (outp.toString().length() > 0) {
                try {
                    Writer outFile = new BufferedWriter(new OutputStreamWriter(
                            new FileOutputStream(new File(theWorkingDirPath + "top_retweet.tsv")), "UTF8"));
                    outFile.write(outp.toString());
                    outFile.close();
                } catch (Exception e) {

                }
               // System.out.println(this.topRetweetArrayJson.toString());
            }
//

        }

        //  System.out.println("Top retweeted mean:"+ topRetweeted.values().stream().mapToInt(Number::intValue).average().toString());
        //  System.out.println("Top retweeted SD:"+ topRetweeted.values().stream().map(Number::doubleValue)
        //          .collect(DoubleStatistics.collector())
        //          .getStandardDeviation());

        topConcurrentHashTags.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(20)
                .forEach((k) -> {
                    out.append(k.getKey() + ": " + k.getValue() + "\n");
//                    System.out.println("#" + k.getKey() + "\t" + k.getValue());
                }); // or any other terminal method


        out.append("\nConcurrent Mention:\n");
        topConcurrentUserMentions.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach((k) -> {
                    out.append(k.getKey() + ": " + k.getValue() + "\n");

                }); // or any other terminal method


        out.append("\nLanguages:\n");
        topLanguages.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(20)
                .forEach((k) -> {
                    out.append(k.getKey() + ": " + k.getValue() + "\n");
                }); // or any other terminal method


        out.append("------------------------");

        Integer totMentions = topConcurrentUserMentions.values().stream().mapToInt(Number::intValue).sum();
        //String formattedString = "Date\tTweets\tRetweets\tReply\tVia\n";
        String formattedString = data + "\t" + tweetsCount + "\t" + retweetsCount + "\t" + directReply + "\t" + totMentions + "\t" + withNativeMedia + "\tn";
        //  formattedString += "\ngraph nodes:" + directedGraph.getNodeCount() + " Edges:" + directedGraph.getEdgeCount();
        return formattedString;
    }


}

class DoubleStatistics extends DoubleSummaryStatistics {

    private double sumOfSquare = 0.0d;
    private double sumOfSquareCompensation; // Low order bits of sum
    private double simpleSumOfSquare; // Used to compute right sum for
    // non-finite inputs

    @Override
    public void accept(double value) {
        super.accept(value);
        double squareValue = value * value;
        simpleSumOfSquare += squareValue;
        sumOfSquareWithCompensation(squareValue);
    }

    public DoubleStatistics combine(DoubleStatistics other) {
        super.combine(other);
        simpleSumOfSquare += other.simpleSumOfSquare;
        sumOfSquareWithCompensation(other.sumOfSquare);
        sumOfSquareWithCompensation(other.sumOfSquareCompensation);
        return this;
    }

    private void sumOfSquareWithCompensation(double value) {
        double tmp = value - sumOfSquareCompensation;
        double velvel = sumOfSquare + tmp; // Little wolf of rounding error
        sumOfSquareCompensation = (velvel - sumOfSquare) - tmp;
        sumOfSquare = velvel;
    }

    public double getSumOfSquare() {
        double tmp = sumOfSquare + sumOfSquareCompensation;
        if (Double.isNaN(tmp) && Double.isInfinite(simpleSumOfSquare)) {
            return simpleSumOfSquare;
        }
        return tmp;
    }

    public final double getStandardDeviation() {
        long count = getCount();
        double sumOfSquare = getSumOfSquare();
        double average = getAverage();
        return count > 0 ? Math.sqrt((sumOfSquare - count * Math.pow(average, 2)) / (count - 1)) : 0.0d;
    }

    public static Collector<Double, ?, DoubleStatistics> collector() {
        return Collector.of(DoubleStatistics::new, DoubleStatistics::accept, DoubleStatistics::combine);
    }

}