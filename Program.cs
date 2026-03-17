using Microsoft.Extensions.Configuration;
using GetMysqlDataToinfluxDb.Services;

try
{

// 加载配置文件
var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

// MySQL 配置
var connectionString = configuration["MySql:ConnectionString"]
    ?? throw new InvalidOperationException("未找到 MySql:ConnectionString 配置项");

// InfluxDB 配置
var influxUrl = configuration["InfluxDB:Url"]
    ?? throw new InvalidOperationException("未找到 InfluxDB:Url 配置项");
var influxDbName = configuration["InfluxDB:DBName"]
    ?? throw new InvalidOperationException("未找到 InfluxDB:DBName 配置项");
var influxUsername = configuration["InfluxDB:Username"] ?? "root";
var influxPassword = configuration["InfluxDB:Password"] ?? "";

// 查询配置
var lookbackMinutes = int.Parse(configuration["Query:LookbackMinutes"] ?? "2");
var syncIntervalSeconds = int.Parse(configuration["Query:SyncIntervalSeconds"] ?? "60");

Console.WriteLine("========================================");
Console.WriteLine("  GetMysqlDataToinfluxDb - 船舶轨迹数据同步");
Console.WriteLine("========================================");
Console.WriteLine($"  回溯时间: {lookbackMinutes} 分钟");
Console.WriteLine($"  同步间隔: {syncIntervalSeconds} 秒");
Console.WriteLine($"  InfluxDB: {influxUrl}/write?db={influxDbName}");
Console.WriteLine();

// 初始化服务
var mysqlService = new MySqlDataService(connectionString);

// 测试数据库连接
if (!await mysqlService.TestConnectionAsync())
{
    Console.WriteLine("[FATAL] 无法连接到 MySQL，程序退出");
    Console.WriteLine("按任意键退出...");
    Console.ReadKey();
    return;
}

// 启动同步工作线程
var syncWorker = new SyncWorker(mysqlService, lookbackMinutes, syncIntervalSeconds,
    influxUrl, influxDbName, influxUsername, influxPassword);
syncWorker.Start();

// 主线程等待 Ctrl+C 退出
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

Console.WriteLine("[INFO] 主线程运行中，按 Ctrl+C 退出程序");
Console.WriteLine();

try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (TaskCanceledException)
{
}

// 停止同步线程
Console.WriteLine("\n[INFO] 正在停止同步线程...");
syncWorker.Stop();
Console.WriteLine("[INFO] 程序已退出");

}
catch (Exception ex)
{
    Console.WriteLine($"[FATAL] 程序异常: {ex.Message}");
    Console.WriteLine("按任意键退出...");
    Console.ReadKey();
}
