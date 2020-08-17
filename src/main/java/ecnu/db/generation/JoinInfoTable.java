package ecnu.db.generation;

import ecnu.db.exception.TouchstoneToolChainException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class JoinInfoTable implements Externalizable {
    /**
     * 复合主键的数量
     */
    private int primaryKeySize;
    /**
     * 最大可以容纳的链表长度，超过该长度会触发压缩
     */
    private static int maxListSize;
    /**
     * join info table，map status -> key list
     */
    private Map<Integer, List<int[]>> joinInfo;

    public JoinInfoTable() {
    }

    public static void setMaxListSize(int maxListSize) {
        JoinInfoTable.maxListSize = maxListSize;
    }

    public JoinInfoTable(int primaryKeySize) {
        this.primaryKeySize = primaryKeySize;
        joinInfo = new HashMap<>();
    }

    public void mergeJoinInfo(JoinInfoTable joinInfoTable) throws TouchstoneToolChainException {
        if (primaryKeySize != joinInfoTable.primaryKeySize) {
            throw new TouchstoneToolChainException("复合主键的size不同");
        }
        joinInfoTable.joinInfo.forEach((key, value) ->
                joinInfo.merge(key, value, (v1, v2) -> {
                    v1.addAll(v2);
                    return v1;
                }));
    }

    /**
     * 根据join status获取符合条件的主键值
     *
     * @param status join status
     * @return 一个复合主键
     */
    public int[] getPrimaryKey(int status) {
        List<int[]> keyList = joinInfo.get(status);
        return keyList.get(ThreadLocalRandom.current().nextInt(keyList.size()));
    }

    /**
     * 插入符合这一status的一组KeyId
     *
     * @param status join status
     * @param keyIds 一个复合主键
     */
    public void addJoinInfo(int status, int[] keyIds) {
        List<int[]> keyList = joinInfo.get(status);
        if (keyList == null) {
            joinInfo.put(status, Collections.singletonList(new int[]{1}));
            joinInfo.put(status, Collections.singletonList(keyIds));
        } else {
            keyList.get(0)[0]++;
            if (keyList.size() > maxListSize) {
                if (ThreadLocalRandom.current().nextDouble() * keyList.get(0)[0] > 1) {
                    keyList.remove(ThreadLocalRandom.current().nextInt(keyList.size() - 1) + 1);
                    keyList.add(keyIds);
                }
            } else {
                keyList.add(keyIds);
            }
        }
    }
    
    /**
     * 清除key list中用于压缩计数的tag
     */
    public void cleanKeyCounter() {
        for (List<int[]> value : joinInfo.values()) {
            value.remove(0);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.write(primaryKeySize);
        out.write(joinInfo.size());
        for (Map.Entry<Integer, List<int[]>> entry : joinInfo.entrySet()) {
            out.write(entry.getKey());
            out.write(entry.getValue().size());
            for (int[] keyIds : entry.getValue()) {
                for (Integer keyId : keyIds) {
                    out.write(keyId);
                }
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        primaryKeySize = in.read();
        int joinInfoSize = in.read();
        joinInfo = new HashMap<>(joinInfoSize);
        for (int i = 0; i < joinInfoSize; i++) {
            ArrayList<int[]> keyList = new ArrayList<>();
            joinInfo.put(in.read(), keyList);
            int keyListSize = in.read();
            for (int j = 0; j < keyListSize; j++) {
                int[] keyId = new int[primaryKeySize];
                for (int k = 0; k < primaryKeySize; k++) {
                    keyId[k] = in.read();
                }
                keyList.add(keyId);
            }
        }
    }
}
