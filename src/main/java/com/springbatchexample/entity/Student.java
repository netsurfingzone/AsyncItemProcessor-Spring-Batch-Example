package com.springbatchexample.entity;

public class Student {

    private int id;
    private String name;
    private String rollNumber;

    public Student() {

    }

    public Student(int id, String name, String rollNumber) {
        this.id = id;
        this.name = name;
        this.rollNumber = rollNumber;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRollNumber() {
        return rollNumber;
    }

    public void setRollNumber(String rollNumber) {
        this.rollNumber = rollNumber;
    }

}