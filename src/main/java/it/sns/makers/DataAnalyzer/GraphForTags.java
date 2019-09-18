package it.sns.makers.DataAnalyzer;

import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
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
import org.gephi.statistics.plugin.EigenvectorCentrality;
import org.gephi.statistics.plugin.GraphDistance;
import org.gephi.statistics.plugin.Modularity;
import org.gephi.utils.progress.ProgressTicket;
import org.gephi.utils.progress.ProgressTicketProvider;
import org.openide.util.Lookup;
import uk.ac.ox.oii.jsonexporter.JSONExporter;

import java.io.*;
import java.util.*;

public class GraphForTags {

    /* graph */
    ProjectController pc;
    GraphModel graphModel;
    UndirectedGraph undirectedGraph;
    Workspace workspace;
    String theWorkingDirPath;
    Integer means = 0;
    Integer stdevs = 0;
    AppearanceController appearanceController;
    AppearanceModel appearanceModel;

    public GraphForTags(String theWorkingDirPath, Integer means, Integer stddevs) {

        this.means = means;
        this.stdevs = stddevs;
        this.pc = Lookup.getDefault().lookup(ProjectController.class);
        pc.newProject();
        this.workspace = pc.getCurrentWorkspace();

        this.graphModel =  Lookup.getDefault().lookup(GraphController.class).getGraphModel(this.workspace);
        this.appearanceController = Lookup.getDefault().lookup(AppearanceController.class);
        this.appearanceModel = appearanceController.getModel();



        this.undirectedGraph = graphModel.getUndirectedGraph();



        this.theWorkingDirPath = theWorkingDirPath;
    }

    public void addHandleNode(String screename) {
        Node aNode = graphModel.factory().newNode(screename);
        aNode.setLabel(screename);
        aNode.setSize(10);
        try {
            undirectedGraph.addNode(aNode);
        } catch (Exception e) {
            //System.err.println("Dup node for scree name " + screename);
        }
    }


    public void addEdge(String source, String target) {
        addHandleNode(source);
        addHandleNode(target);

        if (undirectedGraph.getEdge(undirectedGraph.getNode(source), undirectedGraph.getNode(target)) != null) {
            undirectedGraph.getEdge(undirectedGraph.getNode(source), undirectedGraph.getNode(target)).setWeight(undirectedGraph.getEdge(undirectedGraph.getNode(source), undirectedGraph.getNode(target)).getWeight() + 1.0);
        } else if (undirectedGraph.getEdge(undirectedGraph.getNode(target), undirectedGraph.getNode(source)) != null) {
            undirectedGraph.getEdge(undirectedGraph.getNode(target), undirectedGraph.getNode(source)).setWeight(undirectedGraph.getEdge(undirectedGraph.getNode(target), undirectedGraph.getNode(source)).getWeight() + 1.0);
        } else {
            Edge anEdge = graphModel.factory().newEdge(undirectedGraph.getNode(source), undirectedGraph.getNode(target), false);
            anEdge.setWeight(1.0);
            undirectedGraph.addEdge(anEdge);
        }


    }

    public void analyzeGraph() {

        System.out.println("----------- Hashtag Graph -----------");

        System.out.println("Graph Nodes:" + undirectedGraph.getNodeCount() + "  Edges:" + undirectedGraph.getEdgeCount());
        //  System.out.printf("Desity: %f\n", ((2 * (undirectedGraph.getEdgeCount() - undirectedGraph.getNodeCount() + 1)) / (float) (undirectedGraph.getNodeCount() * (undirectedGraph.getNodeCount() - 3) + 2)));

        GraphDistance gd = new GraphDistance();
        //  gd.execute(undirectedGraph.getModel());

        EigenvectorCentrality ev = new EigenvectorCentrality();
        //  ev.execute(undirectedGraph.getModel());

        Modularity mod = new Modularity();
        //mod.execute(undirectedGraph.getModel());

        //System.out.println("Modularity:" + mod.getModularity());


        // Degree degree = new Degree();
        // degree.execute(undirectedGraph.getModel());
        //  System.out.println("K mean degree:" + degree.getAverageDegree());
    }


    public void exportGraph() {


        /**** colorize graph  ******/

        Column modColumn = this.graphModel.getNodeTable().getColumn(Modularity.MODULARITY_CLASS);
        Function func = appearanceModel.getNodeFunction(undirectedGraph, modColumn, PartitionElementColorTransformer.class);
        if (undirectedGraph.getNodeCount() > 0 ) {
            Partition partition = ((PartitionFunction) func).getPartition();
            Palette palette = PaletteManager.getInstance().generatePalette(partition.size());

            undirectedGraph.getNodes().forEach(node -> {
                node.setColor(palette.getColors()[(Integer) (node.getAttribute("modularity_class"))]);
            });

            undirectedGraph.getEdges().forEach(edge -> {
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
            jex.setExportVisible(false);
            jex.setProgressTicket(ticket);


            //Export only visible graph
            //GraphExporter exporter = (GraphExporter) ec.getExporter("gexf");     //Get GEXF exporter
            //exporter.setExportVisible(true);  //Only exports the visible (filtered) graph
            //exporter.setWorkspace(this.workspace);


            try {
                ec.exportFile(new File(theWorkingDirPath + "hash_gexf.json"), jex);
                // ec.exportFile(new File(theWorkingDirPath + "hash_gexf.gexf"), exporter);
            } catch (IOException ex) {
                ex.printStackTrace();
                return;
            }


            System.out.println("HashTag Graph Exported");
        }else{
            System.out.println("Graph too small");
        }
    }

    public void saveDataForHeatmap() {

        System.out.println("\n----------- Hashtag Graph -----------");

        System.out.println("Graph Nodes:" + undirectedGraph.getNodeCount() + "  Edges:" + undirectedGraph.getEdgeCount());

        NonAsyncModularity mod = new NonAsyncModularity();
        mod.setResolution(1.0);
        mod.setRandom(false);
        mod.setUseWeight(true);

        mod.execute(undirectedGraph.getModel());

        Set<String> nodesSet = new HashSet<>();
        ArrayList<String> nodes = new ArrayList<>();

        Map<Edge, Double> edge_weight_map = new HashMap<>();

        Table<String, String, Double> edgesMatrix = HashBasedTable.create();


        ArrayList<Double> weights = new ArrayList<>();

        for (Edge e : undirectedGraph.getEdges()) {
            edge_weight_map.putIfAbsent(e, e.getWeight());
            weights.add(e.getWeight());
        }

        Mean mn = new Mean();
        StandardDeviation sd = new StandardDeviation();


        Double[] vals = weights.toArray(new Double[weights.size()]);
        double mean_weight = mn.evaluate(Arrays.stream(ArrayUtils.toPrimitive(vals)).toArray());
        double sd_weight = sd.evaluate(Arrays.stream(ArrayUtils.toPrimitive(vals)).toArray());

        double cutoff = (mean_weight * this.means) + (sd_weight * this.stdevs);

        System.out.println("Mean: " + mean_weight);
        System.out.println("Standard deviation: " + sd_weight);


        //apply cutoff

        if (this.means > 0 || this.stdevs > 0) {

            ArrayList<Edge> toBeRemoved = new ArrayList<>();
            Set<Node> toBeRemovedNode = new HashSet<>();


            for (Edge e : undirectedGraph.getEdges()) {
                if (e.getWeight() < cutoff) {
                    toBeRemoved.add(e);
                }
            }
            toBeRemoved.forEach(edge -> {
                undirectedGraph.removeEdge(edge);
            });

            Degree degree = new Degree();
            degree.execute(undirectedGraph.getModel());
            undirectedGraph.getNodes().forEach(node -> {
                if ((Integer) node.getAttribute("degree") == 0) {
                    toBeRemovedNode.add(node);
                }
            });

            toBeRemovedNode.forEach(node -> {
                undirectedGraph.removeNode(node);
            });

            System.out.println("After cutoff -> Graph Nodes:" + undirectedGraph.getNodeCount() + "  Edges:" + undirectedGraph.getEdgeCount());
        }


        System.out.println("\n-------------------------");
        StringBuffer nodes_edges = new StringBuffer();
        edge_weight_map.entrySet().stream()
                .sorted(Map.Entry.<Edge, Double>comparingByValue().reversed())
                .filter((k) -> k.getValue() >= (cutoff))
                .forEach((k) -> {
                    nodes_edges.append(k.getKey().getSource().getLabel() + "\t---\t" + k.getKey().getTarget().getLabel() + "\t" + k.getValue() + "\n");

                });
        try {
            Writer outFile = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(new File(theWorkingDirPath + "hashtag_net_edges.txt")), "UTF8"));

            outFile.write(nodes_edges.toString());
            outFile.close();
        } catch (Exception e) {

        }
        System.out.println("Hashtag net edges text file exported");
        System.out.println("-------------------------");

        for (Edge e : undirectedGraph.getEdges()) {
            if (e.getWeight() > mean_weight + sd_weight) {
                edgesMatrix.put(e.getSource().getLabel(), e.getTarget().getLabel(), e.getWeight());
                nodesSet.add(e.getSource().getLabel());
                nodesSet.add(e.getTarget().getLabel());
            }
        }

        ArrayList<String> nodelist = new ArrayList<>();
        for (Node n : undirectedGraph.getNodes()) {
            nodelist.add(n.getLabel());
        }

        Collections.sort(nodelist);

        nodes.addAll(nodesSet);
        Collections.sort(nodes);
        StringBuffer out = new StringBuffer();

        out.append("Nodes," + Joiner.on(",").join(nodes) + "\n");
        for (int i = 0; i < nodes.size(); i++) {
            out.append(nodes.get(i));
            for (int j = 0; j < nodes.size(); j++) {
                if (edgesMatrix.get(nodes.get(i), nodes.get(j)) != null) {
                    out.append("," + edgesMatrix.get(nodes.get(i), nodes.get(j)));
                } else if (edgesMatrix.get(nodes.get(j), nodes.get(i)) != null) {
                    out.append("," + edgesMatrix.get(nodes.get(j), nodes.get(i)));
                } else {
                    out.append(",0.0");
                }
            }
            out.append("\n");
        }
        try {
            Writer outFile = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(new File(theWorkingDirPath + "matrix.csv")), "UTF8"));

            outFile.write(out.toString());
            outFile.close();

            outFile = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(new File(theWorkingDirPath + "nodelist.txt")), "UTF8"));
            outFile.write(Joiner.on("\n").join(nodelist));
            outFile.close();


        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println(out.toString());

    }
}
