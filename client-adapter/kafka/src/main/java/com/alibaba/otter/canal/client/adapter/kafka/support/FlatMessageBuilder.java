package com.alibaba.otter.canal.client.adapter.kafka.support;

import com.alibaba.otter.canal.protocol.FlatMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author codingxcong
 * @date 2022-09-30
 */
public class FlatMessageBuilder {

    /*public static FlatMessage build(String database, String table, List<Map<String, String>> data) {
        FlatMessage flatMessage = new FlatMessage();
        // 回溯类型
        flatMessage.setType("RECALL");
        flatMessage.setDatabase(database);
        flatMessage.setTable(table);
        flatMessage.setData(data);
        return flatMessage;
    }*/

    public static List<FlatMessage> build(String database, String table, List<Map<String, String>> data) {
        List<FlatMessage> flatMessages = new ArrayList<>();

        List<List<Map<String, String>>> lists = splitList(data, 10);
        for (int i=0; i<lists.size(); i++) {
            FlatMessage flatMessage = new FlatMessage();
            // 回溯类型
            flatMessage.setType("RECALL");
            flatMessage.setDatabase(database);
            flatMessage.setIsDdl(false);
            flatMessage.setTable(table);
            flatMessage.setData(lists.get(i));
            flatMessages.add(flatMessage);
        }

        return flatMessages;
    }


    /**
     * 将集合按len数量分成若干个list
     * @param list
     * @param len 每个集合的数量
     * @return
     */
    public static List<List<Map<String, String>>> splitList(List<Map<String, String>> list, int len) {
        if (list == null || list.size() == 0 || len < 1) {
            return null;
        }
        List<List<Map<String, String>>> result = new ArrayList<List<Map<String, String>>>();

        int size = list.size();
        int count = (size + len - 1) / len;

        for (int i = 0; i < count; i++) {
            List<Map<String, String>> subList = list.subList(i * len, ((i + 1) * len > size ? size : len * (i + 1)));
            result.add(subList);
        }
        return result;
    }

}
