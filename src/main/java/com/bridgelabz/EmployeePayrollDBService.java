package com.bridgelabz;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EmployeePayrollDBService {
    private PreparedStatement employeePayrollDataStatement;

    private static EmployeePayrollDBService employeePayrollDBService;

    private EmployeePayrollDBService() {

    }

    public static EmployeePayrollDBService getInstance() {
        if (employeePayrollDBService == null)
            employeePayrollDBService = new EmployeePayrollDBService();
        return employeePayrollDBService;
    }


    public Connection getConnection() throws EmployeePayrollException {
        Connection con = null;
        try {
            Class.forName("java.sql.DriverManager");
            System.out.println("Driver loaded");
            String username = "root";
            String password = "Admin@123";
            String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
            System.out.println("connecting to database : " + jdbcURL);
            con = DriverManager.getConnection(jdbcURL, username, password);
            System.out.println("connection is successfully " + con);
        } catch (Exception e) {
            throw new EmployeePayrollException(e.getMessage());
        }
        return con;
    }

    public List<EmployeePayrollData> readData() throws EmployeePayrollException {
        String query = "SELECT * FROM employee_payroll; ";
        return this.getEmployeePayrollDataUsingDB(query);

    }

    public List<EmployeePayrollData> getEmployeeData(String name) {
        List<EmployeePayrollData> employeePayrollList = null;
        if (this.employeePayrollDataStatement == null)
            this.preparedStatementForEmployeeData();
        try {
            employeePayrollDataStatement.setString(1, name);
            ResultSet resultSet = employeePayrollDataStatement.executeQuery();
            employeePayrollList = this.getEmployeePayrollData(resultSet);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return employeePayrollList;
    }

    private List<EmployeePayrollData> getEmployeePayrollData(ResultSet resultSet) {
        List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
        EmployeePayrollData emData = new EmployeePayrollData();
        try {
            while (resultSet.next()) {
                emData.setId(resultSet.getInt("id"));
                emData.setName(resultSet.getString("emp_name"));
                emData.setGender(resultSet.getString("gender"));
                emData.setSalary(resultSet.getDouble("salary"));
                emData.setStartDate(resultSet.getDate("start_date").toLocalDate());
                employeePayrollList.add(emData);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return employeePayrollList;
    }


    public List<EmployeePayrollData> getEmployeePayrollForDateRange(LocalDate startDate, LocalDate endDate) throws EmployeePayrollException {
        String query = String.format("SELECT * FROM employee_payroll WHERE start_date BETWEEN '%s' AND '%s';", Date.valueOf(startDate), Date.valueOf(endDate));
        return this.getEmployeePayrollDataUsingDB(query);
    }

    public Map<String, Double> getAverageSalaryByGender() throws EmployeePayrollException {
        String query = "SELECT gender, AVG(salary) as avg_salary FROM employee_payroll GROUP BY gender;";
        Map<String, Double> genderToAverageSalaryMap = new HashMap<>();
        try (Connection connection = this.getConnection();) {
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(query);
            while (result.next()) {
                String gender = result.getString("gender");
                double salary = result.getDouble("avg_salary");
                genderToAverageSalaryMap.put(gender, salary);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return genderToAverageSalaryMap;
    }

    private List<EmployeePayrollData> getEmployeePayrollDataUsingDB(String query) throws EmployeePayrollException {
        List<EmployeePayrollData> employeePayrollList = new ArrayList<>();

        try (Connection connection = this.getConnection();) {
            EmployeePayrollData emData = new EmployeePayrollData();
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(query);
            employeePayrollList = this.getEmployeePayrollData(result);
        } catch (SQLException e) {
            throw new EmployeePayrollException(e.getMessage());
        }
        return employeePayrollList;
    }

    private void preparedStatementForEmployeeData() {
        try {
            Connection connection = this.getConnection();
            String query = "SELECT * FROM employee_payroll where name = ?";
            employeePayrollDataStatement = connection.prepareStatement(query);
        } catch (EmployeePayrollException | SQLException e) {
            e.printStackTrace();
        }
    }

    public int updateData(String name, double salary) throws EmployeePayrollException, SQLException {
        return this.updateEmployeeDataUsingStatement(name, salary);
    }

    private int updateEmployeeDataUsingStatement(String name, double salary) throws EmployeePayrollException {
        String query = String.format("update employee_payroll set salary = %.2f where name = '%s';", salary, name);
        try (Connection connection = this.getConnection()) {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(query);
        } catch (SQLException e) {
            throw new EmployeePayrollException(e.getMessage());
        }
    }

    public EmployeePayrollData addEmployeeToPayrollUC7(String name, double salary, LocalDate startDate, String gender) throws EmployeePayrollException {
        int employeeId = -1;
        EmployeePayrollData employeePayrollData = new EmployeePayrollData();
        String query = String.format("INSERT INTO employee_payroll (emp_name,gender,salary,start_date)"
                + "VALUES ('%s','%s','%s','%s')", name, gender, salary, Date.valueOf(startDate));
        try (Connection connection = this.getConnection()) {
            Statement statement = connection.createStatement();
            int rowAffected = statement.executeUpdate(query, statement.RETURN_GENERATED_KEYS);
            if (rowAffected == 1) {
                ResultSet resultSet = statement.getGeneratedKeys();
                if (resultSet.next()) employeeId = resultSet.getInt(1);
            }
            employeePayrollData.setId(employeeId);
            employeePayrollData.setName(name);
            employeePayrollData.setSalary(salary);
            employeePayrollData.setGender(gender);
            employeePayrollData.setStartDate(startDate);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return employeePayrollData;
    }

    public EmployeePayrollData addEmployeeToPayroll(String emp_name, double salary, LocalDate startDate, String gender) throws EmployeePayrollException, SQLException {
        int employeeId = -1;
        Connection connection = null;
        EmployeePayrollData employeePayrollData = new EmployeePayrollData();
        try {
            connection = this.getConnection();
            connection.setAutoCommit(false);
        } catch (EmployeePayrollException e) {
            e.printStackTrace();
        }
        try (Statement statement = connection.createStatement()) {
            String query = String.format("INSERT INTO employee_payroll (name,gender,salary,start_date)"
                    + "VALUES ('%s','%s','%s','%s')", emp_name, gender, salary, Date.valueOf(startDate));
            int rowAffected = statement.executeUpdate(query, statement.RETURN_GENERATED_KEYS);
            if (rowAffected == 1) {
                ResultSet resultSet = statement.getGeneratedKeys();
                if (resultSet.next()) employeeId = resultSet.getInt(1);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            connection.rollback();
            return employeePayrollData;
        }

        try (Statement statement = connection.createStatement()) {
            double deductions = salary * 0.2;
            double taxablePay = salary - deductions;
            double tax = taxablePay * 0.1;
            double netPay = salary - tax;
            String query = String.format("INSERT INTO payroll_details (Id,basic_pay,deductions,taxable_pay,tax,net_pay)"
                    + "VALUES (%s, %s, %s, %s, %s,%s)", employeeId, salary, deductions, taxablePay, tax, netPay);

            int rowAffected = statement.executeUpdate(query);
            if (rowAffected == 1) {
                employeePayrollData.setId(employeeId);
                employeePayrollData.setName(emp_name);
                employeePayrollData.setSalary(salary);
                employeePayrollData.setGender(gender);
                employeePayrollData.setStartDate(startDate);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            connection.rollback();
        }
        try {
            connection.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        return employeePayrollData;
    }
}
