package org.example;

import java.io.IOException;

public interface DBapi {
    public String query(String key) throws IOException;
    public String update(String key, String val) throws IOException;
}
