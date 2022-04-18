package com.bridgelabz;
import java.time.LocalDate;
import java.util.Objects;

public class EmployeePayrollData {
    private int id;
    private String name;
    private  String gender;
    private double salary;
    private LocalDate startDate;

    public EmployeePayrollData(){

    }
    public EmployeePayrollData(int id, String name, String gender, double salary, LocalDate startDate) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.salary = salary;
        this.startDate = startDate;
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

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }

    @Override
    public String toString() {
        return "EmployeePayrollData{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", gender='" + gender + '\'' +
                ", salary=" + salary +
                ", startDate=" + startDate +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmployeePayrollData that = (EmployeePayrollData) o;
        return id == that.id && Double.compare(that.salary, salary) == 0 && Objects.equals(name, that.name) && Objects.equals(gender, that.gender) && Objects.equals(startDate, that.startDate);
    }

}

