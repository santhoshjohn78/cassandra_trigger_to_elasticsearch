package ehss.cassandra.triggers.elasticsearch;
/**
 * @author EHSS
 * This is a cassandra trigger that transfers the cassandra row into ES index.
 */

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ElasticSearchTrigger implements ITrigger{

    static Logger logger = LoggerFactory.getLogger(ElasticSearchTrigger.class);

    static RestHighLevelClient restHighLevelClient;

    private static Properties properties = new Properties();
    private static String esHost;
    private static Integer esPort;

    static {
        try {
            properties.load(ElasticSearchTrigger.class.getClassLoader().getResourceAsStream("application.properties"));
            esHost = properties.getProperty("elasticsearch.host");
            esPort = Integer.parseInt(properties.getProperty("elasticsearch.port"));
        }catch(IOException ioe){
            ioe.printStackTrace();
            throw new RuntimeException(ioe);
        }
        logger.info("Elasticsearch host:"+esHost);
        logger.info("Elasticsearch port:"+esPort);

        restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(esHost,
                esPort)).build());

    }

    public Collection<Mutation> augment(Partition partition) {
        String keyspace = partition.metadata().ksName;
        String tableName = partition.metadata().cfName;
        logger.info("Column Family: " + tableName);
        logger.info("Keyspace : " + keyspace);

        JSONObject obj = new JSONObject();
        String partitionKey = partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
        obj.put("partition_key", partitionKey);

        try {
            UnfilteredRowIterator it = partition.unfilteredIterator();

            while (it.hasNext()) {
                Unfiltered un = it.next();
                Clustering clt = (Clustering) un.clustering();
                logger.info(un.toString(partition.metadata(),true));
                Iterator<Cell> cells = partition.getRow(clt).cells().iterator();
                Iterator<ColumnDefinition> columns = partition.getRow(clt).columns().iterator();

                while(columns.hasNext()){
                    ColumnDefinition columnDef = columns.next();
                    Cell cell = cells.next();
                    String data = new String(cell.value().array());
                    obj.put(columnDef.name, data);
                    logger.info(columnDef.name+" , "+data);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
        }
        logger.info("Before sending to ElasticSearch...");
        logger.info(obj.toString());
        try {
            index(tableName, keyspace, partitionKey, obj.toJSONString());
            logger.info("After sending to ElasticSearch!!!");
        }catch(Exception ex){
            logger.error("Transfer to ES failed",ex);
        }


        return Collections.emptyList();
    }


    public void index(String index, String type, String id, String jsonString) throws Exception{
        IndexRequest request = new IndexRequest(index,type,id);

        request.source(jsonString, XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(request);
    }

    public void index(String index, String type, String id, Map<String, Object> jsonMap ) throws IOException {
        IndexRequest request = new IndexRequest(index,type,id);

        request.source(jsonMap);
        IndexResponse indexResponse = restHighLevelClient.index(request);
    }

    public void index(String index, String type, String id, XContentBuilder jsonBuilder) throws IOException {
        IndexRequest request = new IndexRequest(index,type,id);

        request.source(jsonBuilder);
        IndexResponse indexResponse = restHighLevelClient.index(request);
    }
}
