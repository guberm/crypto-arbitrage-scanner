using ArbitrageScanner.Interfaces;
using ArbitrageScanner.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NLog;
namespace ArbitrageScanner
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    var logger = LogManager.Setup()
                        .LoadConfigurationFromFile("nlog.config") 
                        .GetCurrentClassLogger();
                    services.AddSingleton(logger);

                    services.AddSingleton<IExchangeService, ExchangeService>();
                    services.AddScoped<IDBService, ClickhouseDBService>();
                    services.AddSingleton<CollectorService>();

                    // Adding background services
                    services.AddHostedService(provider => provider.GetRequiredService<CollectorService>());
                    services.AddHostedService<VerificationService>();
                })
                .Build();

            await host.RunAsync(); // Launching the host
        }
    }
}
