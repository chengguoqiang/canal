package com.alibaba.otter.canal.store.helper;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 相关的操作工具
 * 
 * @author jianghang 2012-6-19 下午05:49:21
 * @version 1.0.0
 */
public class CanalEventUtils {

    /**
     * 找出一个最小的position位置，相等的情况返回position1
     */
    public static LogPosition min(LogPosition position1, LogPosition position2) {
        if (position1.getIdentity().equals(position2.getIdentity())) {
            // 首先根据文件进行比较
            if (position1.getPosition().getJournalName().compareTo(position2.getPosition().getJournalName()) > 0) {
                return position2;
            } else if (position1.getPosition().getJournalName().compareTo(position2.getPosition().getJournalName()) < 0) {
                return position1;
            } else {
                // 根据offest进行比较
                if (position1.getPosition().getPosition() > position2.getPosition().getPosition()) {
                    return position2;
                } else {
                    return position1;
                }
            }
        } else {
            // 不同的主备库，根据时间进行比较
            if (position1.getPosition().getTimestamp() > position2.getPosition().getTimestamp()) {
                return position2;
            } else {
                return position1;
            }
        }
    }

    /**
     * 根据entry创建对应的Position对象
     */
    public static LogPosition createPosition(Event event) {
        EntryPosition position = new EntryPosition();
        position.setJournalName(event.getEntry().getHeader().getLogfileName());
        position.setPosition(event.getEntry().getHeader().getLogfileOffset());
        position.setTimestamp(event.getEntry().getHeader().getExecuteTime());
        // add serverId at 2016-06-28
        position.setServerId(event.getEntry().getHeader().getServerId());

        LogPosition logPosition = new LogPosition();
        logPosition.setPosition(position);
        logPosition.setIdentity(event.getLogIdentity());
        return logPosition;
    }

    /**
     * 根据entry创建对应的Position对象
     */
    public static LogPosition createPosition(Event event, boolean included) {
        EntryPosition position = new EntryPosition();
        position.setJournalName(event.getEntry().getHeader().getLogfileName());
        position.setPosition(event.getEntry().getHeader().getLogfileOffset());
        position.setTimestamp(event.getEntry().getHeader().getExecuteTime());
        position.setIncluded(included);

        LogPosition logPosition = new LogPosition();
        logPosition.setPosition(position);
        logPosition.setIdentity(event.getLogIdentity());
        return logPosition;
    }

    /**
     * 判断当前的entry和position是否相同
     */
    public static boolean checkPosition(Event event, LogPosition logPosition) {
        EntryPosition position = logPosition.getPosition();
        CanalEntry.Entry entry = event.getEntry();
        boolean result = position.getTimestamp().equals(entry.getHeader().getExecuteTime());

        boolean exactely = (StringUtils.isBlank(position.getJournalName()) && position.getPosition() == null);
        if (!exactely) {// 精确匹配
            result &= StringUtils.equals(entry.getHeader().getLogfileName(), position.getJournalName());
            result &= position.getPosition().equals(entry.getHeader().getLogfileOffset());
        }

        return result;
    }
}
