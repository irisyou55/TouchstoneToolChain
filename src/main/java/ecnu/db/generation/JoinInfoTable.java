package ecnu.db.generation;

import ecnu.db.exception.TouchstoneToolChainException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Math.*;

public class JoinInfoTable implements Externalizable {
    /**
     * 复合主键的数量
     */
    private int primaryKeySize;
    /**
     * 最大可以容纳的链表长度，超过该长度会触发压缩
     */
    private static int maxListSize = 1000;
    /**
     * join info table，map status -> key list
     */
    private ConcurrentMap<Long, List<int[]>> joinInfo = new ConcurrentHashMap<>();

    private final ConcurrentMap<Long, Long> counters = new ConcurrentHashMap<>();

    private final ConcurrentMap<Long, Double> paramWs = new ConcurrentHashMap<>();

    private final ConcurrentMap<Long, Long> nextIdxs = new ConcurrentHashMap<>();

    public JoinInfoTable() {}

    public static void setMaxListSize(int maxListSize) {
        JoinInfoTable.maxListSize = maxListSize;
    }

    public JoinInfoTable(int primaryKeySize) {
        this.primaryKeySize = primaryKeySize;
    }

    public void mergeJoinInfo(JoinInfoTable toMergeTable) throws TouchstoneToolChainException {
        if (primaryKeySize != toMergeTable.primaryKeySize) {
            throw new TouchstoneToolChainException("复合主键的size不同");
        }
        toMergeTable.joinInfo.forEach((k, v) -> {
            joinInfo.merge(k, v, (v1, v2) -> {
                v1.addAll(v2);
                return v1;
            });
        });
    }

    /**
     * 根据join status获取符合条件的主键值
     *
     * @param status join status
     * @return 一个复合主键
     */
    public int[] getPrimaryKey(long status) {
        List<int[]> keyList = joinInfo.get(status);
        return keyList.get(ThreadLocalRandom.current().nextInt(keyList.size()));
    }

    /**
     * 根据join status获取符合条件的主键值
     * @param status join status
     * @return 所有复合主键
     */
    public List<int[]> getAllKeys(long status) {
        return joinInfo.get(status);
    }

    /**
     * 插入符合这一status的一组KeyId
     * reservoir sampling following Algorithm L in "Reservoir-Sampling Algorithms of Time Complexity O(n(1+log(N/n)))"
     *
     * @param status join status
     * @param key 一个复合主键
     */
    public void addJoinInfo(long status, int[] key) {
        counters.compute(status, (s1, counts) -> {
           if (counts == null) {
               counts = 0L;
           }
           if (counts < maxListSize) {
               joinInfo.compute(status, (s2, keys) -> {
                   if (keys == null) {
                       keys = new ArrayList<>(maxListSize);
                   }
                   keys.add(key);
                   return keys;
               });
           }
           else {
               double paramW = paramWs.compute(status, (s2, w) -> {
                   if (w == null) {
                       w = exp(logRandom() / maxListSize);
                   }
                   return w;
               });
               double finalWeight = paramW;
               long nextIdx = nextIdxs.compute(status, (s3, idx) -> {
                    if (idx == null) {
                        idx = generateNextIdx(finalWeight, maxListSize);
                    }
                    return idx;
               });
               if (counts == nextIdx) {
                   joinInfo.computeIfPresent(status, (s2, keys) -> {
                       int j = ThreadLocalRandom.current().nextInt(0, maxListSize);
                       keys.set(j, key);
                       return keys;
                   });
                   nextIdx = generateNextIdx(paramW, nextIdx);
                   paramW = paramW * exp(logRandom() / maxListSize);
                   paramWs.put(status, paramW);
               }
               nextIdxs.put(status, nextIdx);
           }
           counts++;
           return counts;
        });
    }

    /**
     * 用于计算下一个可以插入的idx
     * @param weight W参数
     * @param previousIdx 前一个可以插入的idx
     * @return 下一个可以插入的idx
     */
    private static long generateNextIdx(double weight, long previousIdx) {
        return previousIdx + ((long) (floor(logRandom() / log(1 - weight)))) + 1;
    }

    /**
     * 计算一个log(random()), 出于防止overflow的目的, 设置了一个下界
     * @return log(random())
     */
    private static double logRandom() {
        return log(ThreadLocalRandom.current().nextDouble(1e-7, 1));
    }

    /**
     * 清除key list中用于压缩计数的tag
     */
    public void cleanKeyCounter() {
        counters.clear();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.write(primaryKeySize);
        out.write(joinInfo.size());
        for (Long bitmap : joinInfo.keySet()) {
            out.writeLong(bitmap);
            List<int[]> keys = joinInfo.get(bitmap);
            out.write(keys.size());
            for (int[] keyIds : keys) {
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
        joinInfo = new ConcurrentHashMap<>(joinInfoSize);
        for (int i = 0; i < joinInfoSize; i++) {
            Long bitmap = in.readLong();
            int keyListSize = in.read();
            for (int j = 0; j < keyListSize; j++) {
                int[] keyId = new int[primaryKeySize];
                for (int k = 0; k < primaryKeySize; k++) {
                    keyId[k] = in.read();
                }
                joinInfo.compute(bitmap, (b, keys) -> {
                    if (keys == null) {
                        keys = new ArrayList<>(maxListSize);
                    }
                    keys.add(keyId);
                    return keys;
                });
            }
        }
    }
}
