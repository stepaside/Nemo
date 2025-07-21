using System;
using Microsoft.Data.SqlClient;

namespace Nemo.Benchmark
{
    public static class DatabaseSetup
    {
        public static void CreateNorthwindDatabase()
        {
            var password = Environment.GetEnvironmentVariable("SA_PASSWORD") ?? "YourPasswordHere";
            var masterConnectionString = $"Data Source=localhost,1433;Initial Catalog=master;User Id=sa;Password={password};TrustServerCertificate=true";
            var northwindConnectionString = $"Data Source=localhost,1433;Initial Catalog=Northwind;User Id=sa;Password={password};TrustServerCertificate=true";

            using (var connection = new SqlConnection(masterConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'Northwind') CREATE DATABASE Northwind";
                    command.ExecuteNonQuery();
                }
            }

            using (var connection = new SqlConnection(northwindConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Customers' AND xtype='U')
                        CREATE TABLE Customers (
                            CustomerID NVARCHAR(5) PRIMARY KEY,
                            CompanyName NVARCHAR(40) NOT NULL,
                            ContactName NVARCHAR(30),
                            ContactTitle NVARCHAR(30),
                            Address NVARCHAR(60),
                            City NVARCHAR(15),
                            Region NVARCHAR(15),
                            PostalCode NVARCHAR(10),
                            Country NVARCHAR(15),
                            Phone NVARCHAR(24),
                            Fax NVARCHAR(24)
                        );

                        IF NOT EXISTS (SELECT * FROM Customers)
                        INSERT INTO Customers VALUES 
                        ('ALFKI', 'Alfreds Futterkiste', 'Maria Anders', 'Sales Representative', 'Obere Str. 57', 'Berlin', NULL, '12209', 'Germany', '030-0074321', '030-0076545'),
                        ('ANATR', 'Ana Trujillo Emparedados y helados', 'Ana Trujillo', 'Owner', 'Avda. de la Constitución 2222', 'México D.F.', NULL, '05021', 'Mexico', '(5) 555-4729', '(5) 555-3745'),
                        ('ANTON', 'Antonio Moreno Taquería', 'Antonio Moreno', 'Owner', 'Mataderos 2312', 'México D.F.', NULL, '05023', 'Mexico', '(5) 555-3932', NULL),
                        ('AROUT', 'Around the Horn', 'Thomas Hardy', 'Sales Representative', '120 Hanover Sq.', 'London', NULL, 'WA1 1DP', 'UK', '(171) 555-7788', '(171) 555-6750'),
                        ('BERGS', 'Berglunds snabbköp', 'Christina Berglund', 'Order Administrator', 'Berguvsvägen 8', 'Luleå', NULL, 'S-958 22', 'Sweden', '0921-12 34 65', '0921-12 34 67'),
                        ('BLAUS', 'Blauer See Delikatessen', 'Hanna Moos', 'Sales Representative', 'Forsterstr. 57', 'Mannheim', NULL, '68306', 'Germany', '0621-08460', '0621-08924'),
                        ('BLONP', 'Blondesddsl père et fils', 'Frédérique Citeaux', 'Marketing Manager', '24, place Kléber', 'Strasbourg', NULL, '67000', 'France', '88.60.15.31', '88.60.15.32'),
                        ('BOLID', 'Bólido Comidas preparadas', 'Martín Sommer', 'Owner', 'C/ Araquil, 67', 'Madrid', NULL, '28023', 'Spain', '(91) 555 22 82', '(91) 555 91 99'),
                        ('BONAP', 'Bon app''', 'Laurence Lebihan', 'Owner', '12, rue des Bouchers', 'Marseille', NULL, '13008', 'France', '91.24.45.40', '91.24.45.41'),
                        ('BOTTM', 'Bottom-Dollar Markets', 'Elizabeth Lincoln', 'Accounting Manager', '23 Tsawassen Blvd.', 'Tsawassen', 'BC', 'T2F 8M4', 'Canada', '(604) 555-4729', '(604) 555-3745');";
                    command.ExecuteNonQuery();
                }
            }

            Console.WriteLine("Northwind database and Customers table created successfully.");
        }
    }
}
