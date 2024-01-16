package com.mercari.solution.util.domain.search;

import com.mercari.solution.util.gcp.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class H2Util {

    private static final Logger LOG = LoggerFactory.getLogger(H2Util.class);

    public static class Config implements Serializable {

        private String input;
        private String table;
        private List<String> ddls;
        private List<String> keyFields;
        private JdbcUtil.OP op;

        public String getInput() {
            return input;
        }

        public String getTable() {
            return table;
        }

        public List<String> getDdls() {
            return ddls;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public JdbcUtil.OP getOp() {
            return op;
        }

        public List<String> validate(int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("H2.configs[" + index + "].input parameter must not be null.");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(table == null) {
                this.table = this.input;
            }
            if(ddls == null) {
                ddls = new ArrayList<>();
            }
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(op == null) {
                op = JdbcUtil.OP.INSERT;
            }
        }
    }

}
