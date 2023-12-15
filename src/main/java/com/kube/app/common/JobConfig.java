//package com.kube.app.common;
//
//import lombok.AllArgsConstructor;
//import lombok.Builder;
//import lombok.Getter;
//import lombok.NoArgsConstructor;
//import org.apache.flink.api.common.ExecutionConfig;
//
//import java.io.Serializable;
//import java.util.HashMap;
//import java.util.Map;
//
//@Builder
//public class JobConfig extends ExecutionConfig.GlobalJobParameters implements Serializable {
//
//    private static final long serialVersionUID = 1L;
//
//    @Getter private Map<Integer, Long> partitionOffsets;
//
//    @Override
//    public Map<String, String> toMap() {
//        return new HashMap<>(){{
//            put("partitionOffsets", partitionOffsets.toString());
//        }};
//    }
//
//    public static void main(String[] args) {
//        JobConfig config = new JobConfigBuilder()
//                .partitionOffsets(new HashMap<>())
//                .build();
//        config.getPartitionOffsets();
//    }
//}
