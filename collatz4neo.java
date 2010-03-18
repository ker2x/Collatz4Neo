package org.keru.collatz4neo;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;

public class Collatz4Neo {

    final String dbPath = "c:/neocollatz3-1000k/";
    final int COLLATZ_START = 1;
    final int COLLATZ_END = 1000000;
    final int TRANSACTION_SIZE = 1000;
    int collatz = 1;
    int nextVal;
    int t = 0;

    Node cNode, nextNode;

    EmbeddedGraphDatabase neo;
    LuceneIndexService index;
    Node referenceNode;
    Transaction tx;

    enum linkType implements RelationshipType {
        LINK_TO
    }

    public void calcCollatz(int c) {
        //System.out.println(c);
        if(c == 1) return;

        if (c%2 == 0) {
            nextVal = c/2;
        } else {
            nextVal = 3*c +1;
        }

        //tx = neo.beginTx();
        cNode = index.getSingleNode("n", c);
        nextNode = index.getSingleNode("n", nextVal);
        //tx.success(); tx.finish();
        
        if(nextNode == null) {
            //nextNode = createnode
            //tx = neo.beginTx();
             nextNode = neo.createNode();
             nextNode.setProperty("n", nextVal);
             index.index(nextNode, "n", nextVal);
            //tx.success(); tx.finish();
        } else {
            if(cNode == null) {
                //cNode = createnode
                //tx = neo.beginTx();
                  cNode = neo.createNode();
                  cNode.setProperty("n", c);
                  index.index(cNode, "n", c);
                  //LinkToNode
                  cNode.createRelationshipTo(nextNode, linkType.LINK_TO);
                //tx.success(); tx.finish();
            } else {
                //LinkToNode
                //tx = neo.beginTx();
                  cNode.createRelationshipTo(nextNode, linkType.LINK_TO);
                //tx.success(); tx.finish();
            }
            return;
        }

        if(cNode == null) {
            //tx = neo.beginTx();
              cNode = neo.createNode();
              cNode.setProperty("n", c);
              index.index(cNode, "n", c);
              //LinkToNode
              cNode.createRelationshipTo(nextNode, linkType.LINK_TO);
            //tx.success(); tx.finish();
        } else {
            //LinkToNode
            //tx = neo.beginTx();
              cNode.createRelationshipTo(nextNode, linkType.LINK_TO);
            //tx.success(); tx.finish();
        }

        if (c%2 == 0) {
            calcCollatz(c/2);
        } else {
            calcCollatz(3*c +1);
        }

    }

    public void runCollatz() {
        System.out.println("Initializeing DB");
        neo =  new EmbeddedGraphDatabase(dbPath);

        System.out.println("Initializing Lucene Index service");
        index = new LuceneIndexService(neo);
        index.enableCache("n", 100000);

        System.out.println("Get reference node");
        tx = neo.beginTx();
        referenceNode = this.neo.getReferenceNode();
        tx.success(); tx.finish();

        while(collatz < COLLATZ_END) {
            System.out.println(collatz);
            t = 0;
            tx = neo.beginTx();
            while(t < TRANSACTION_SIZE) {
                calcCollatz(collatz);
                collatz++;
                t++;
            }
            tx.success(); tx.finish();
        }
        


        //Exiting
        index.shutdown();
        neo.shutdown();
        System.out.println("finished");
        return;
    }

    public static void main(String[] args) {

        Collatz4Neo c4n = new Collatz4Neo();
        c4n.runCollatz();
        System.out.println("exiting");
        System.exit(0);

    }

}

