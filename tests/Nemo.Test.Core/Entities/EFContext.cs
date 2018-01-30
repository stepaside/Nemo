using Microsoft.EntityFrameworkCore;
using Nemo.Configuration;

namespace NemoTest
{
    public class EFContext : DbContext
    {
        public DbSet<Customer> Customers { get; set; }
        public DbSet<Order> Orders { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            var settings = ConfigurationFactory.DefaultConfiguration.SystemConfiguration.ConnectionString("DbConnection");
            optionsBuilder.UseSqlServer(settings.ConnectionString);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Customer>().Property(x => x.Id).HasColumnName("CustomerID");
            modelBuilder.Entity<Customer>().HasKey(x => x.Id);
            modelBuilder.Entity<Order>().Property(x => x.CustomerId).HasColumnName("CustomerID");
            modelBuilder.Entity<Order>().HasKey(x => x.OrderId);
        }
    }
}
