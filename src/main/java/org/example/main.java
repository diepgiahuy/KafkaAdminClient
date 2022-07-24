package org.example;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class main {
    public enum FilePath {
        Topic {
            public String toString() {
                return "topic.json";
            }
        },

        Broker {
            public String toString() {
                return "broker.json";
            }
        },

        Partition {
            public String toString() {
                return "partition.json";
            }
        },

        Diff {
            public String toString() {
                return "diff.json";
            }
        },
        BrokerConfig {
            public String toString() {
                return "broker_config.json";
            }
        },
        TopicConfig {
            public String toString() {
                return "topic_config.json";
            }
        },
    }

    enum Action {
        ADD, DELETE
    }
    public static CreatePartitionsResult addPartitionsForTopic(AdminClient adminClient, String topic, int partitions) {
        Map<String, NewPartitions> map = new HashMap<>();
        NewPartitions np = NewPartitions.increaseTo(partitions);
        map.put(topic, np);
        CreatePartitionsOptions cpo = new CreatePartitionsOptions();
        cpo.timeoutMs(5 * 1000);
        return adminClient.createPartitions(map, cpo);
    }

    public static void compareState(List<LinkedHashMap<String, Object>> prevState, List<LinkedHashMap<String, Object>> currState,String uniqueKey, String type, List<LinkedHashMap<String,Object>> diffBetweenStates) throws IOException {

        Set<LinkedHashMap<String, Object>> setPrevState= new HashSet<>(prevState);
        Set<LinkedHashMap<String, Object>> setCurrState = new HashSet<>(currState);

        if(setCurrState.size() == 0) {
            // CurrState remove all
            prevState.forEach( object -> {
                LinkedHashMap<String,Object> diff = new LinkedHashMap<>();
                diff.put("type",type);
                diff.put("action",Action.DELETE.name());
                diff.put("value", object.toString());
                diffBetweenStates.add(diff);
            });
            return;
        }

        Set<LinkedHashMap<String, Object>> setCachePrevState = new HashSet<>(setPrevState);
         if (setPrevState.equals(setCurrState)) {
             // no change
             return;
         }
         setPrevState.removeAll(setCurrState);
         setCurrState.removeAll(setCachePrevState);
         currState = new ArrayList<>(setCurrState);
         prevState = new ArrayList<>(setPrevState);
         if(setPrevState.size() == 0) {
             // CurrState add something
             currState.forEach( object -> {
                 LinkedHashMap<String,Object> diff = new LinkedHashMap<>();
                 diff.put("change",type);
                 diff.put("action",Action.ADD.name());
                 diff.put("value", object.toString());
                 diffBetweenStates.add(diff);
             });
             return;
         }

         // Find modify
        for(int i = 0 ; i < setPrevState.size(); i++) {
            boolean isContain = false;
            for(int j = 0 ; j < currState.size();j++) {
                String valuePrevUniqueKey = prevState.get(i).get(uniqueKey).toString();
                String valueCurrUniqueKey = currState.get(j).get(uniqueKey).toString();

                if(valuePrevUniqueKey.equals(valueCurrUniqueKey)) {
                    // Checked for modify
                    MapDifference<String, Object> mapDiff = Maps.difference(prevState.get(i), currState.get(j));
                    Map<String, MapDifference.ValueDifference<Object>> entriesDiffering = mapDiff.entriesDiffering();
                    entriesDiffering.forEach((k, v) -> {
                        LinkedHashMap<String,Object> diff = new LinkedHashMap<>();
                        diff.put("change",type);
                        diff.put("unique_key",uniqueKey + ":" +valuePrevUniqueKey);
                        diff.put("modify_key",k);
                        diff.put("prev_Value", v.leftValue());
                        diff.put("curr_Value", v.rightValue());
                        diffBetweenStates.add(diff);
                    });
                    // remove modify from currState => currState will contain only add object if it has
                        if(mapDiff.entriesOnlyOnLeft().size() > 0) {
                            mapDiff.entriesOnlyOnLeft().forEach((k,v) -> {
                                LinkedHashMap<String,Object> diff = new LinkedHashMap<>();
                                diff.put("change",type);
                                diff.put("action",Action.DELETE.name());
                                diff.put("unique_key",uniqueKey + ":" +valuePrevUniqueKey);
                                diff.put("delete_key", k);
                                diff.put("delete_value", v);
                                diffBetweenStates.add(diff);
                            });
                        }
                        if(mapDiff.entriesOnlyOnRight().size() > 0) {
                            mapDiff.entriesOnlyOnRight().forEach((k,v) -> {
                                LinkedHashMap<String,Object> diff = new LinkedHashMap<>();
                                diff.put("change",type);
                                diff.put("action",Action.ADD.name());
                                diff.put("unique_key",uniqueKey + ":" + valueCurrUniqueKey);
                                diff.put("add_key", k);
                                diff.put("add_value", v);
                                diffBetweenStates.add(diff);
                            });
                        }
                    currState.remove(j);
                    j--;
                    isContain = true;
                }
            }
            if(!isContain) {
                LinkedHashMap<String,Object> diff = new LinkedHashMap<>();
                diff.put("change",type);
                diff.put("action",Action.DELETE.name());
                diff.put("value", prevState.get(i));
                diffBetweenStates.add(diff);
            }
        }
        currState.forEach( object -> {
            LinkedHashMap<String,Object> diff = new LinkedHashMap<>();
            diff.put("change",type);
            diff.put("action",Action.ADD.name());
            diff.put("value", object);
            diffBetweenStates.add(diff);
        });

    }

    public static boolean isFileEmpty(File file) {
        return file.length() == 0;
    }

    public static  void main(String[] args) throws ExecutionException, InterruptedException {

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19091");
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "test-client-2");
        AdminClient admin = AdminClient.create(config);

        List<LinkedHashMap<String,Object>> listMapCurrentBroker = new ArrayList<>();
        List<LinkedHashMap<String,Object>> listMapCurrentBrokerConfig = new ArrayList<>();
        List<LinkedHashMap<String,Object>> listMapCurrentTopics = new ArrayList<>();
        List<LinkedHashMap<String,Object>> listMapCurrentTopicConfig = new ArrayList<>();
        List<LinkedHashMap<String,Object>> listMapCurrentPartitions = new ArrayList<>();


        for (Node node : admin.describeCluster().nodes().get()) {
            LinkedHashMap<String,Object> mapCurrentBrokerConfig = new LinkedHashMap<>();
            mapCurrentBrokerConfig.put(ConfigResource.Type.BROKER.toString(),node.idString());
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult dcr = admin.describeConfigs(Collections.singleton(cr));
            dcr.all().get().forEach((k, c) -> {
                c.entries()
                        .forEach(configEntry -> {
                            mapCurrentBrokerConfig.put(configEntry.name(),configEntry.value());
                        });
            });
            LinkedHashMap<String,Object> mapCurrentBrokers = new LinkedHashMap<>();
            mapCurrentBrokers.put(ConfigResource.Type.BROKER.toString(),node.idString());
            mapCurrentBrokers.put("host",node.host());
            mapCurrentBrokers.put("id",node.idString());
            mapCurrentBrokers.put("rack", String.valueOf(node.hasRack()));
            listMapCurrentBroker.add(mapCurrentBrokers);
            listMapCurrentBrokerConfig.add(mapCurrentBrokerConfig);
        }

        for (TopicListing topicListing : admin.listTopics().listings().get()) {
            LinkedHashMap<String,Object> mapCurrentTopics = new LinkedHashMap<>();
            mapCurrentTopics.put("name",topicListing.name());
            mapCurrentTopics.put("id",topicListing.topicId().toString());
            mapCurrentTopics.put("internal", String.valueOf(topicListing.isInternal()));
            listMapCurrentTopics.add(mapCurrentTopics);

            ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topicListing.name());
            DescribeConfigsResult dcr = admin.describeConfigs(Collections.singleton(cr));
            LinkedHashMap<String,Object> mapCurrentTopicConfig = new LinkedHashMap<>();
            dcr.all().get().forEach((k, c) -> {
                c.entries()
                        .forEach(configEntry -> {
                            mapCurrentTopicConfig.put("Topic",topicListing.name());
                            mapCurrentTopicConfig.put(configEntry.name(),configEntry.value());
                        });
            });
            listMapCurrentTopicConfig.add(mapCurrentTopicConfig);

            DescribeTopicsResult describeTopicsResult = admin.describeTopics(Collections.singleton(topicListing.name()));
            List<TopicPartitionInfo> partitionInfos = describeTopicsResult.allTopicNames().get().values().iterator().next().partitions();
            LinkedHashMap<String,Object> mapCurrentPartitions = new LinkedHashMap<>();
            partitionInfos.forEach(p -> {
                mapCurrentPartitions.put("Topic",topicListing.name());
                mapCurrentPartitions.put("partition", String.valueOf(p.partition()));
                mapCurrentPartitions.put("leader", p.leader().toString());
                mapCurrentPartitions.put("replicas",p.replicas().toString());
                mapCurrentPartitions.put("isr", p.isr().toString());
            });
            listMapCurrentPartitions.add(mapCurrentPartitions);
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            List<LinkedHashMap<String,Object>> diffBetweenStates = new ArrayList<>();
            File BrokerConfigFile = new File(FilePath.BrokerConfig.toString());
            if(!isFileEmpty(BrokerConfigFile)) {
                List<LinkedHashMap<String, Object>> prevState = mapper.readValue( BrokerConfigFile,new TypeReference<List<LinkedHashMap<String, Object>>>(){});
                List<LinkedHashMap<String, Object>> currState = new ArrayList<>(listMapCurrentBrokerConfig);
                compareState(prevState,currState,"BROKER","Broker_Config",diffBetweenStates);
            }

            File BrokerFile = new File(FilePath.Broker.toString());
            if(!isFileEmpty(BrokerFile)) {
                List<LinkedHashMap<String, Object>> prevState = mapper.readValue( BrokerFile,new TypeReference<List<LinkedHashMap<String, Object>>>(){});
                List<LinkedHashMap<String, Object>> currState = new ArrayList<>(listMapCurrentBroker);
                compareState(prevState,currState,"BROKER","Broker",diffBetweenStates);
            }

            File TopicConfigFile = new File(FilePath.TopicConfig.toString());
            if(!isFileEmpty(TopicConfigFile)) {
                List<LinkedHashMap<String, Object>> prevState = mapper.readValue( TopicConfigFile,new TypeReference<List<LinkedHashMap<String, Object>>>(){});
                List<LinkedHashMap<String, Object>> currState = new ArrayList<>(listMapCurrentTopicConfig);
                compareState(prevState,currState,"Topic","Topic_Config",diffBetweenStates);
            }

            File TopicFile = new File(FilePath.Topic.toString());
            if(!isFileEmpty(TopicConfigFile)) {
                List<LinkedHashMap<String, Object>> prevState = mapper.readValue( TopicFile,new TypeReference<List<LinkedHashMap<String, Object>>>(){});
                List<LinkedHashMap<String, Object>> currState = new ArrayList<>(listMapCurrentTopics);
                compareState(prevState,currState,"id","Topic",diffBetweenStates);
            }

            File PartitionFile = new File(FilePath.Partition.toString());
            if(!isFileEmpty(PartitionFile)) {
                List<LinkedHashMap<String, Object>> prevState = mapper.readValue( PartitionFile,new TypeReference<List<LinkedHashMap<String, Object>>>(){});
                List<LinkedHashMap<String, Object>> currState = new ArrayList<>(listMapCurrentPartitions);
                compareState(prevState,currState,"Topic","Partition",diffBetweenStates);
            }

            if (diffBetweenStates.size() > 0) {
                mapper.writeValue(new File(FilePath.Diff.toString()), diffBetweenStates);
            }

            mapper.writeValue(new File(FilePath.Topic.toString()), listMapCurrentTopics);
            mapper.writeValue(new File(FilePath.Broker.toString()), listMapCurrentBroker);
            mapper.writeValue(new File(FilePath.BrokerConfig.toString()), listMapCurrentBrokerConfig);
            mapper.writeValue(new File(FilePath.TopicConfig.toString()), listMapCurrentTopicConfig);
            mapper.writeValue(new File(FilePath.Partition.toString()), listMapCurrentPartitions);


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}