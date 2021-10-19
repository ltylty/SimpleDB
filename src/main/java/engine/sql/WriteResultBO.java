package engine.sql;

import engine.net.response.SelectResponse;
import io.netty.buffer.ByteBuf;

/**
 * @Author: 刘天元
 * @Date: 2021/10/19 19:05
 */
public class WriteResultBO {
    private SelectResponse selectResponse;
    private ByteBuf buffer;
    private Integer count;

    public WriteResultBO(SelectResponse selectResponse, ByteBuf buffer, Integer count) {
        this.selectResponse = selectResponse;
        this.buffer = buffer;
        this.count = count;
    }

    public SelectResponse getSelectResponse() {
        return selectResponse;
    }

    public void setSelectResponse(SelectResponse selectResponse) {
        this.selectResponse = selectResponse;
    }

    public ByteBuf getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
