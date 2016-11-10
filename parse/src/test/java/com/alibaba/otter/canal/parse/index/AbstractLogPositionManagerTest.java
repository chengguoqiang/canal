package com.alibaba.otter.canal.parse.index;

import java.net.InetSocketAddress;
import java.util.Date;

import org.junit.Assert;

import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;

public class AbstractLogPositionManagerTest extends AbstractZkTest {

    private static final String MYSQL_ADDRESS = "127.0.0.1";

    public LogPosition doTest(CanalLogPositionManager logPositionManager) {
        LogPosition getPosition = logPositionManager.getLatestIndexBy(destination);
        Assert.assertNull(getPosition);

        LogPosition position1 = buildPosition(1);
        logPositionManager.persistLogPosition(destination, position1);
        LogPosition getPosition1 = logPositionManager.getLatestIndexBy(destination);
        Assert.assertEquals(position1, getPosition1);

        LogPosition position2 = buildPosition(2);
        logPositionManager.persistLogPosition(destination, position2);
        LogPosition getPosition2 = logPositionManager.getLatestIndexBy(destination);
        Assert.assertEquals(position2, getPosition2);
        return position2;
    }

    protected LogPosition buildPosition(int number) {
        LogPosition position = new LogPosition();
        position.setIdentity(new LogIdentity(new InetSocketAddress(MYSQL_ADDRESS, 3306), 1234L));
        position.setPosition(new EntryPosition("mysql-bin.000000" + number, 106L, new Date().getTime()));
        return position;
    }
}
