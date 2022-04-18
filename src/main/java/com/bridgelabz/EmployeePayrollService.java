package com.bridgelabz;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;


public class EmployeePayrollService {
    private List<EmployeePayrollData> employeePayrollList;

    private EmployeePayrollDBService employeePayrollDBService;

    public List<EmployeePayrollData> getEmployeePayrollList() {
        return employeePayrollList;
    }

    public EmployeePayrollService(){
        employeePayrollDBService = EmployeePayrollDBService.getInstance();
    }

    public EmployeePayrollService(List<EmployeePayrollData> employeePayrollList){
        this();
        this.employeePayrollList = employeePayrollList;
    }

    public static void main(String[] args) throws EmployeePayrollException {
        EmployeePayrollService employeePayrollServiceMain = new EmployeePayrollService();
        employeePayrollServiceMain.retrievingEmployeeData();
    }

    public List<EmployeePayrollData> retrievingEmployeeData() throws EmployeePayrollException {
        this.employeePayrollList = employeePayrollDBService.readData();
        return this.employeePayrollList;
    }

    public void updateEmployeeSalary(String name, double salary) throws SQLException, EmployeePayrollException {
        int result = employeePayrollDBService.updateData(name,salary);
        if ( result == 0 ) return;
        EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
        if (employeePayrollData != null) employeePayrollData.setSalary(salary);
    }

    private EmployeePayrollData getEmployeePayrollData(String name) {
        EmployeePayrollData employeePayrollData;
        employeePayrollData = this.employeePayrollList.stream()
                .filter(employeePayrollDataItem -> employeePayrollDataItem.getName().equals(name))
                .findFirst()
                .orElse(null);
        return employeePayrollData;
    }

    public boolean checkEmployeePayrollInSyncWithDB(String name) {
        List<EmployeePayrollData> employeePayrollDataList = employeePayrollDBService.getEmployeeData(name);
        return employeePayrollDataList.get(0).equals(getEmployeePayrollData(name));
    }

    public List<EmployeePayrollData> readEmployeePayrollForDate(LocalDate startDate, LocalDate endDate) throws EmployeePayrollException {
        return  employeePayrollDBService.getEmployeePayrollForDateRange(startDate,endDate);
    }

    public Map<String, Double> readAverageSalaryByGender() throws EmployeePayrollException {
        return employeePayrollDBService.getAverageSalaryByGender();
    }

    public void addEmployeeToPayroll(String name, double salary, LocalDate startDate, String gender) throws EmployeePayrollException {
        try {
            employeePayrollList.add(employeePayrollDBService.addEmployeeToPayroll(name,salary,startDate,gender));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
