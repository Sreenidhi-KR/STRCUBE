package org.example;

public class DBConfig {
    String url = "jdbc:mysql://172.16.201.190:3306/STRCUBE?sessionVariables=sql_mode='NO_ENGINE_SUBSTITUTION'&jdbcCompliantTruncation=false&createDatabaseIfNotExist=true";
    String username = "venkatesh"; // replace with your username
    String password = "KVrsmck@21"; // replace with your password


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
