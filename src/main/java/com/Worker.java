package com;

public class Worker {
    private String id;

    private String name;

    private int port;

    private String status;

    public Worker(String id, String name, int port, String status) {
        this.id = id;
        this.name = name;
        this.port = port;
        this.status = status;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }
}
