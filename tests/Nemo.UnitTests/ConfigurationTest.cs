using Nemo.Configuration;

namespace Nemo.UnitTests
{
    /// <summary>
    /// Because configuration is static, these tests are not thread-safe. I aim to fix that in the future - DJL 8/7/2025
    /// </summary>
    [TestClass]
    public class ConfigurationTest
    {
        [TestMethod]
        public void Default_Config_Test()
        {
            var config = ConfigurationFactory.DefaultConfiguration;
            Assert.IsFalse(config.AutoTypeCoercion);
        }

        [TestMethod]
        public void Configure_Test()
        {
            var config = ConfigurationFactory.Configure()
                .SetAutoTypeCoercion(true);
            Assert.IsTrue(config.AutoTypeCoercion);
        }

        [TestMethod]
        public void Configure_Lazy_Test()
        {
            ConfigurationFactory.ConfigureLazy(c => c
                .SetAutoTypeCoercion(true));
            var config = ConfigurationFactory.DefaultConfiguration;
            Assert.IsTrue(config.AutoTypeCoercion);
        }
    }
}
