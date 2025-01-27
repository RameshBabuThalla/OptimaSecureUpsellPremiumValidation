using DocumentFormat.OpenXml.Spreadsheet;
using OptimaSecureUpsellPremiumValidation.BussinessLogic;
using OptimaSecureUpsellPremiumValidation.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System.Configuration;
using System;
using OptimaSecureUpsellPremiumValidation.Models.Domain;
using Microsoft.Extensions.Logging;
using Serilog;
using Microsoft.Extensions.Hosting;
using OptimaSecureUpsellPremiumValidation;
using Serilog.Core;
using DocumentFormat.OpenXml.Office2016.Drawing.ChartDrawing;
using System.Data;

var builder = Host.CreateDefaultBuilder(args);
string logFilePath = @"C:\temp\OS_UPSELLLog\app_log.txt"; // Or any known writable directory
Directory.CreateDirectory(Path.GetDirectoryName(logFilePath));

Log.Information("OS_UPSELL Application has started.");

// Configure Serilog
Log.Logger = new LoggerConfiguration().MinimumLevel.Information()
    .WriteTo.Console(outputTemplate: "{Timestamp:HH:mm:ss} [{Level}] {Message}{NewLine}{Exception}")  // Customize console output
    .WriteTo.File(logFilePath, rollingInterval: RollingInterval.Hour,
                  outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level}] {Message}{NewLine}{Exception}")  // Customize file output format
    .Filter.ByExcluding(logEvent =>
        logEvent.Properties.ContainsKey("SourceContext") &&
        logEvent.Properties["SourceContext"].ToString().Contains("Microsoft.EntityFrameworkCore.Database.Command") &&
        logEvent.Level == Serilog.Events.LogEventLevel.Information &&
        logEvent.MessageTemplate.Text.Contains("Executed DbCommand")  // Exclude logs that contain 'Executed DbCommand'
    )
    .CreateLogger();

string connectionString = ConfigurationManager.ConnectionStrings["PostgresDb"]?.ConnectionString;


if (string.IsNullOrEmpty(connectionString))
{
    Console.WriteLine("Connection string is missing from app.config");
    return;
}

//// Configure Services & Dependency Injection
builder.ConfigureServices((context, services) =>
{
 
    services.AddLogging(configure => configure.AddSerilog());

    services.AddSingleton<IDbConnection>(sp => new NpgsqlConnection(connectionString));

    // Register DbContext with PostgreSQL
    services.AddDbContext<HDFCDbContext>(options =>
        options.UseNpgsql(connectionString));

    services.AddTransient<OptimaSecure>();
  
    services.AddHostedService<MyWorker>();
});

var host = builder.Build();


builder.ConfigureServices((context, services) =>
{
    services.AddLogging(configure => configure.AddConsole());
    services.AddHostedService<MyWorker>();
    services.AddTransient<Program>();
    services.AddSingleton<OptimaSecure>(); 
   
});

Console.WriteLine("Schedular is Started!");
Console.WriteLine("OS Upsell Premium Validation Schedular Started!");
AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
var serviceProvider = new ServiceCollection().AddLogging(logging => logging.AddSerilog())    
    .AddDbContext<HDFCDbContext>(options =>
        options.UseNpgsql(connectionString))                                          
    .AddTransient<OptimaSecure>()
    .BuildServiceProvider();
var optimaSecure = serviceProvider.GetService<OptimaSecure>();
string postgresConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["PostgresDb"].ConnectionString;
using (var postgresConnection = new NpgsqlConnection(postgresConnectionString))
{
    try
    {
        postgresConnection.Open();       
       
            try
            {
                List<string> idPlaceholders = new List<string>();          
                var listofpolicies = optimaSecure.FetchNewBatchIds(postgresConnection);
            using (var scope = host.Services.CreateScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<HDFCDbContext>();
                var baserates = await optimaSecure.GetRatesAsync(dbContext);
                var relations = await optimaSecure.GetRelationTagsAsync(dbContext);
                var cirates = await optimaSecure.GetCIRatesTagsAsync(dbContext);
                var carates = await optimaSecure.GetCARatesTagsAsync(dbContext);
                var hdcrates = await optimaSecure.GetHDCRatesTagsAsync(dbContext);
                var hdcproportionsplit = await optimaSecure.GetHDCProportionSplitTagsAsync(dbContext);
                var deductableDiscount = await optimaSecure.GetDedutableDiscountAsync(dbContext);

                if (listofpolicies.Count > 0)
                {
                    var tasks = new List<System.Threading.Tasks.Task>();
                    {
                        var semaphore = new SemaphoreSlim(10);

                        foreach (var item in listofpolicies)
                        {
                            var task = System.Threading.Tasks.Task.Run(async () =>
                            {
                                await semaphore.WaitAsync();
                                try
                                {
                                    string certificateNo = item[0];                                   
                                    var osRNEDataSecure = await optimaSecure.GetOptimaSecureValidation(certificateNo, baserates, relations, cirates, hdcrates, hdcproportionsplit, deductableDiscount);
                                }
                                finally
                                {
                                    semaphore.Release();  // Release the semaphore after the task is done
                                }
                            });
                            tasks.Add(task);
                        }
                        await System.Threading.Tasks.Task.WhenAll(tasks);
                    }
                }
            }
            }
            catch (Exception ex)
            {               
                Log.Error(ex, "An error occurred while processing calculating premium.");
                Console.WriteLine("Error occurred: " + ex.Message);
            }
        

    }

    catch (Exception ex)
    {
        // Rollback the transaction in case of error
        // transaction.Rollback();
        Log.Error(ex, "An error occurred while processing the application.");
        Console.WriteLine("Error occurred: " + ex.Message);
    }

}

Console.WriteLine("Schedular is Completed!");
Log.Information("Application has finished processing.");
EmailService.SendEmail();
Log.CloseAndFlush();




