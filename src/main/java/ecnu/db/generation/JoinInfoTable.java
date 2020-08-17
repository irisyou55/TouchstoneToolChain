package ecnu.db.generation;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import ecnu.db.exception.TouchstoneToolChainException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private ListMultimap<Long, int[]> joinInfo = ArrayListMultimap.create();

    private final Map<Long, Integer> metCounter = new HashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

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
        lock.readLock().lock();
        joinInfo.putAll(toMergeTable.joinInfo);
        lock.readLock().unlock();
    }

    /**
     * 根据join status获取符合条件的主键值
     *
     * @param status join status
     * @return 一个复合主键
     */
    public int[] getPrimaryKey(long status) {
        lock.readLock().lock();
        List<int[]> keyList = joinInfo.get(status);
        int[] ret = keyList.get(ThreadLocalRandom.current().nextInt(keyList.size()));
        lock.readLock().unlock();
        return ret;
    }

    /**
     * 根据join status获取符合条件的主键值
     * @param status join status
     * @return 所有复合主键
     */
    public List<int[]> getAllKeys(long status) {
        lock.readLock().lock();
        List<int[]> keyList = joinInfo.get(status);
        lock.readLock().unlock();
        return keyList;
    }

    /**
     * 插入符合这一status的一组KeyId
     *
     * @param status join status
     * @param key 一个复合主键
     */
    public void addJoinInfo(long status, int[] key) {
        lock.writeLock().lock();
        List<int[]> keys = joinInfo.get(status);
        if (keys.size() < maxListSize) {
            joinInfo.put(status, key);
        } else {
            Integer i = metCounter.getOrDefault(status, maxListSize);
            i++;
            metCounter.put(status, i);
            int j = ThreadLocalRandom.current().nextInt(0, i);
            if (j < maxListSize) {
                keys.set(j, key);
            }
        }
        lock.writeLock().unlock();
    }

    /**
     * 清除key list中用于压缩计数的tag
     */
    public void cleanKeyCounter() {
        lock.writeLock().lock();
        metCounter.clear();
        lock.writeLock().lock();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        lock.readLock().lock();
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
        lock.readLock().lock();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        lock.writeLock().lock();
        primaryKeySize = in.read();
        int joinInfoSize = in.read();
        joinInfo = ArrayListMultimap.create(joinInfoSize, maxListSize);
        for (int i = 0; i < joinInfoSize; i++) {
            Long bitmap = in.readLong();
            int keyListSize = in.read();
            for (int j = 0; j < keyListSize; j++) {
                int[] keyId = new int[primaryKeySize];
                for (int k = 0; k < primaryKeySize; k++) {
                    keyId[k] = in.read();
                }
                joinInfo.put(bitmap, keyId);
            }
        }
        lock.writeLock().unlock();
    }
}
