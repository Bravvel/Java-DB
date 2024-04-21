import javax.net.ssl.SSLContextSpi;
import java.io.IOException;
import java.sql.*;
import java.util.Scanner;

public class Database {
    public static final String USER = "root";
    public static final String PASSWORD = "A2003ujlz";
    public static final String URL = "jdbc:mysql://localhost:3306/mysql";
    public static Connection conection;
    public static Statement statement;

    static {
        try {
            conection = DriverManager.getConnection(URL,USER,PASSWORD); //Просим у ДрайверМенеджера что бы он дал нам соединение
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException();
        }
    }
    static {
        try {
            statement = conection.createStatement(); //создаём Statement
        }catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException();
        }
    }
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        Scanner scanner = new Scanner(System.in);
        Class.forName("com.mysql.cj.jdbc.Driver"); //загружается драйвер JDBC для MySQL
        statement.executeUpdate("USE customersandorders"); //выполняется запрос на использование базы данных

        while(true){
            System.out.println("Введите команду, которую хотите выполнить");
            System.out.println("1 - Сделать выборку");
            System.out.println("2 - Сделать запрос на обновление");
            System.out.println("3 - Сделать запрос на добавление");
            System.out.println("4 - Сделать запрос на удаление");
            System.out.println("0 - Завершить работу программы");
            int command = scanner.nextInt();
            switch(command){
                case 0:
                    return;
                case 1:
                    selectMenu();
                    break;
                case 2:
                    updateMenu();
                    break;
                case 3:
                    insertMenu();
                    break;
                case 4:
                    deleteMenu();
                    break;
                default:
                    System.out.println("Такой комманды не существует");
                    break;
            }
        }

    }
    public static void printQuery(ResultSet rs) throws SQLException { // метод для вывода данных из исполненного запроса
        ResultSetMetaData rsmd = rs.getMetaData();
        int numColumns = rsmd.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= numColumns; i++) {
                System.out.printf(rs.getString(i) + " ");
            }
            System.out.print("\n");
        }
    }

    public static void selectMenu(){
        Scanner scanner = new Scanner(System.in);
        System.out.println("Какую выборку вы хотите сделать?");
        System.out.println("0 - Ввести запрос самостоятелно");
        System.out.println("1 - Вывести все записи в определенной таблице");
        System.out.println("2 - Вывести все товары, цена которых выше заданной");
        System.out.println("3 - Вывести все товары, у которых цена выше среднего");
        System.out.println("4 - Вывести итоговое кол-во заказов и кол-во заказанных товаров");
        System.out.println("5 - Вывести среднюю стоимость товаров");
        System.out.println("6 - Количество заказов в каждый день");
        System.out.println("7 - Получить информацию о заказчиках, включая количество заказов и общую стоимость заказов");
        System.out.println("8 - Выбрать определенные столбцы из таблицы:");
        System.out.println("9 - Выбрать строки из таблицы с определенным условием:");
        System.out.println("10 - Посчитать кол-во записей в таблице");
        System.out.println("11 - Вывести всех покупателей, которые купили определенный продукт");

        String table = new String();
        String column1 = new String();
        String column2 = new String();
        String condition = new String();
        int selectCommand = scanner.nextInt();

        ResultSet result;

        try {
            switch (selectCommand) {
                case 0:
                    System.out.println("Введите запрос");
                    String request = new String();
                    request = scanner.next();
                    request += scanner.nextLine();
                    result = statement.executeQuery(request);
                    printQuery(result);
                    break;
                case 1:
                    System.out.println("Введите название таблицы");
                    table = scanner.next();
                    result = statement.executeQuery("SELECT * FROM " + table);
                    printQuery(result);
                    break;
                case 2:
                    System.out.println("Введите цену");
                    double price = scanner.nextDouble();
                    result = statement.executeQuery("SELECT * FROM products WHERE product_price > " + price);
                    printQuery(result);
                    break;
                case 3:
                    result = statement.executeQuery("SELECT * FROM products WHERE product_price > (SELECT " +
                            "AVG(product_price) FROM products);");
                    printQuery(result);
                    break;
                case 4:
                    result = statement.executeQuery("SELECT COUNT(*) AS total_orders, SUM(quantity) AS total_items" +
                            " FROM order_items;");
                    printQuery(result);
                    break;
                case 5:
                    result = statement.executeQuery("SELECT AVG(product_price) AS avg_price FROM products;");
                    printQuery(result);
                    break;
                case 6:
                    result = statement.executeQuery("SELECT order_date, COUNT(*) AS total_orders FROM orders" +
                            " GROUP BY order_date;");
                    printQuery(result);
                    break;
                case 7:
                    result = statement.executeQuery("SELECT \n" +
                            "  customers.customer_id, \n" +
                            "  customers.customer_name, \n" +
                            "  COUNT(orders.order_id) AS num_orders, \n" +
                            "  SUM(order_items.quantity * products.product_price) AS total_cost \n" +
                            "FROM \n" +
                            "  customers\n" +
                            "  JOIN orders ON customers.customer_id = orders.customer_id \n" +
                            "  JOIN order_items ON orders.order_id = order_items.order_id \n" +
                            "  JOIN products ON order_items.product_id = products.product_id \n" +
                            "GROUP BY \n" +
                            "  customers.customer_id;");
                    printQuery(result);
                    break;
                case 8:
                    System.out.println("Введите название таблицы и двух столбцов (в одну строку разделяя пробелом)");
                    table = scanner.next();
                    column1 = scanner.next();
                    column2 = scanner.next();
                    result = statement.executeQuery("SELECT " + column1 + ", " + column2 + " FROM " + table);
                    printQuery(result);
                    break;
                case 9:
                    System.out.println("Введите название таблицы и ограничение в одну строку");
                    table = scanner.next();
                    condition = scanner.nextLine();
                    result = statement.executeQuery("SELECT * FROM " + table + " WHERE " + condition);
                    printQuery(result);
                    break;
                case 10:
                    System.out.println("Введите название таблицы");
                    table = scanner.next();
                    result = statement.executeQuery("SELECT COUNT(*) FROM " + table + ";");
                    printQuery(result);
                    break;
                case 11:
                    System.out.println("Введите название товара");
                    String product = scanner.next();
                    result = statement.executeQuery("SELECT\n" +
                            "customers.customer_name,\n" +
                            "orders.order_id,\n" +
                            "products.product_name\n" +
                            "FROM\n" +
                            "customers\n" +
                            "JOIN orders ON customers.customer_id = orders.customer_id\n" +
                            "JOIN order_items ON orders.order_id = order_items.order_id\n" +
                            "JOIN products ON order_items.product_id = products.product_id\n" +
                            "WHERE products.product_name = '" + product + "';");
                    printQuery(result);
                    break;
                default:
                    System.out.println("Такого запроса на выборку не существует");
                    break;
            }
        } catch (SQLException e){
            System.out.println("Не удалось сделать выборку, проверьте корректность входных данных");
        }
    }
    public static void updateMenu(){
        Scanner scanner = new Scanner(System.in);
        System.out.println("Какой запрос на обновление вы хотите сделать ?");
        System.out.println("0 - Ввести запрос самостоятелно");
        System.out.println("1 - Обновить одно поле в таблице используя условие");
        System.out.println("2 - Обновить несколько полей в таблице используя условие");

        String table = new String();
        String column1 = new String();
        String column2 = new String();
        String condition = new String();

        int updateCommand = scanner.nextInt();

        try {
            switch (updateCommand) {
                case 0:
                    System.out.println("Введите запрос");
                    String request = new String();
                    request = scanner.next();
                    request += scanner.nextLine();
                    statement.executeUpdate(request);
                    break;
                case 1:
                    String newValue = new String();
                    System.out.println("Введите название таблицы, название стобца, новое значение и условие через пробел");
                    table = scanner.next();
                    column1 = scanner.next();
                    newValue = scanner.next();
                    condition = scanner.nextLine();
                    statement.executeUpdate("UPDATE " + table + " SET " + column1 + " = " + "'" + newValue + "'" +
                            " WHERE " + condition + ";");
                    System.out.println("Данные были успешно обновлены");
                    break;
                case 2:
                    System.out.println("Введите название таблицы, двух стобцов, два новых значения для соответствующих" +
                            " столбцов и условие через пробел");
                    String newValue1 = new String();
                    String newValue2 = new String();
                    table = scanner.next();
                    column1 = scanner.next();
                    column2 = scanner.next();
                    newValue1 = scanner.next();
                    newValue2 = scanner.next();
                    condition = scanner.nextLine();
                    statement.executeUpdate("UPDATE " + table + " SET " + column1 + " = " + "'" + newValue1 + "'"
                            + ", " + column2 + " = " + "'" + newValue2 + "'" + ", " + column1 + " = " + "'" + newValue1
                            + "'" + " WHERE " + condition + ";");
                    System.out.println("Данные были успешно обновлены");
                default:
                    System.out.println("Такой команды не существует");
                    break;
            }
        } catch (SQLException e){
            System.out.println("Не удалось выполнить запрос на обновление, проверьте корректность входных данных");
        }
    }
    public static void insertMenu(){
        Scanner scanner = new Scanner(System.in);
        String newValue1 = new String();
        String newValue2 = new String();
        String newValue3 = new String();

        System.out.println("Какой запрос на добавление данных вы хотите сделать ?");
        System.out.println("1 - добавить запись в customers");
        System.out.println("2 - добавить запись в orders");
        System.out.println("3 - добавить запись в order_items");
        System.out.println("4 - добавить запись в products");

        int insertCommand = scanner.nextInt();

        try {
            switch (insertCommand) {
                case 1:
                    System.out.println("Введите значения, которые хотитие добавить в таблицу customers");
                    newValue1 = scanner.next();
                    newValue2 = scanner.next();
                    newValue3 = scanner.next();

                    statement.executeUpdate("INSERT INTO customers (customer_id, customer_name, customer_email)"
                            + " VALUE (" + newValue1 + ", " + "'" + newValue2 + "'" + ", "
                            + newValue3 + ");");
                    System.out.println("Данные были успешно добвлены");
                    break;
                case 2:
                    System.out.println("Введите значения, которые хотитие добавить в таблицу orders");
                    newValue1 = scanner.next();
                    newValue2 = scanner.next();
                    newValue3 = scanner.next();

                    statement.executeUpdate("INSERT INTO orders (order_id, order_date, customer_id)"
                            + " VALUE (" + newValue1 + ", " + "'" + newValue2 + "'" + ", "
                            + newValue3 + ");");
                    System.out.println("Данные были успешно добвлены");
                    break;
                case 3:
                    System.out.println("Введите значения, которые хотитие добавить в таблицу orders_items");
                    newValue1 = scanner.next();
                    newValue2 = scanner.next();
                    newValue3 = scanner.next();

                    statement.executeUpdate("INSERT INTO orders_items (order_id, product_id, quantity)"
                            + " VALUE (" + newValue1 + ", " + "'" + newValue2 + "'" + ", "
                            + newValue3 + ");");
                    System.out.println("Данные были успешно добвлены");
                    break;
                case 4:
                    System.out.println("Введите значения, которые хотитие добавить в таблицу products");
                    newValue1 = scanner.next();
                    newValue2 = scanner.next();
                    newValue3 = scanner.next();

                    statement.executeUpdate("INSERT INTO products (product_id, product_name, product_price)"
                            + " VALUE (" + newValue1 + ", " + "'" + newValue2 + "'" + ", "
                            + newValue3 + ");");
                    System.out.println("Данные были успешно добвлены");
                    break;
                default:
                    System.out.println("Такой команды не существует");
                    break;
            }
        } catch (SQLException e){
            System.out.println("Не удалось выполнить запрос на добавление, проверьте корректность входных данных");
        }

    }

    public static void deleteMenu() {

        Scanner scanner = new Scanner(System.in);

        System.out.println("Какой запрос на обновление вы хотите сделать ?");
        System.out.println("0 - Ввести запрос самостоятелно");
        System.out.println("1 - Удаление всех кортежей, удовлетворяющих заданному условию");
        System.out.println("2 - Удаление всех данных из таблицы");

        String condition = new String();
        String table = new String();
        int deleteCommand = scanner.nextInt();

        try {
            switch (deleteCommand) {
                case 0:
                    System.out.println("Введите запрос");
                    String request = new String();
                    request = scanner.next();
                    request += scanner.nextLine();
                    statement.executeUpdate(request);
                    break;
                case 1:
                    System.out.println("Введите название таблицы и условие через пробел");
                    table = scanner.next();
                    condition = scanner.nextLine();
                    statement.executeUpdate("DELETE FROM " + table + " WHERE " + condition + ";");
                    System.out.println("Данные были успешно удалены");
                    break;
                case 2:
                    table = scanner.next();
                    statement.executeUpdate("DELETE * FROM " + table + ";");
                    System.out.println("Данные были успешно удалены");
                    break;
                default:
                    System.out.println("Такой команды не существует");
            }
        } catch(SQLException e){
            System.out.println("Не удалось выполнить запрос на удаление данных");
        }
    }
}