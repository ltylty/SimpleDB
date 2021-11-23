package server.sql;

import server.net.handler.frontend.FrontendConnection;
import server.net.proto.util.Fields;
import server.net.response.SelectResponse;
import io.netty.buffer.ByteBuf;
import engine.Field;
import engine.Tuple;
import engine.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * @Author: 刘天元
 * @Date: 2021/10/19 16:53
 */

public class WriteResult implements BiConsumer<Tuple, WriteResultBO> {

    FrontendConnection con;

    public WriteResult(FrontendConnection con) {
        this.con = con;
    }

    @Override
    public void accept(Tuple tup, WriteResultBO writeResultBO) {
        SelectResponse selectResponse = writeResultBO.getSelectResponse();
        ByteBuf buffer = writeResultBO.getBuffer();
        Integer count = writeResultBO.getCount();

        if (count == 0) {
            if (con != null) {
                // field
                writeFields(tup, con, selectResponse, buffer);
                // eof
                selectResponse.writeEof(con, buffer);
            }
        }
        // rows
        if (con != null) {
            List<String> values = new ArrayList<>();
            Iterator<Field> fieldIterator = tup.fields();
            while (fieldIterator.hasNext()) {
                values.add(fieldIterator.next().toString());
            }
            selectResponse.writeRow(values, con, buffer);
        }

        count++;

        writeResultBO.setSelectResponse(selectResponse);
        writeResultBO.setBuffer(buffer);
        writeResultBO.setCount(count);
    }

    public void writeFields(Tuple tup, FrontendConnection con, SelectResponse
            selectResponse, ByteBuf buffer) {
        for (int i = 0; i < tup.getTupleDesc().numFields(); i++) {
            // 默认是string
            int type = convertValueTypeToFieldType(tup.getTupleDesc().getFieldType(i));
            String fieldName = tup.getTupleDesc().getFieldName(i);
            selectResponse.addField(fieldName, type);
        }
        selectResponse.responseFields(con, buffer);
    }

    public static int convertValueTypeToFieldType(Type valueType) {
        if (valueType == Type.INT_TYPE) {
            return Fields.FIELD_TYPE_INT24;
        } else {
            return Fields.FIELD_TYPE_STRING;
        }
    }


}
