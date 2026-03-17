using System.Text;
using GetMysqlDataToinfluxDb.Models;

namespace GetMysqlDataToinfluxDb.Services;

/// <summary>
/// 数据同步工作线程
/// 独立线程中循环：读取 MySQL → 解析字段 → 推送 InfluxDB
/// </summary>
public class SyncWorker
{
    private readonly MySqlDataService _mysqlService;
    private readonly int _lookbackMinutes;
    private readonly int _syncIntervalSeconds;
    private readonly string _influxWriteUrl;
    private readonly string _influxAuthHeader;
    private readonly HttpClient _httpClient;
    private Thread? _workerThread;
    private CancellationTokenSource? _cts;

    public SyncWorker(MySqlDataService mysqlService, int lookbackMinutes, int syncIntervalSeconds,
        string influxUrl, string influxDbName, string influxUsername, string influxPassword)
    {
        _mysqlService = mysqlService;
        _lookbackMinutes = lookbackMinutes;
        _syncIntervalSeconds = syncIntervalSeconds;
        _influxWriteUrl = $"{influxUrl}/write?db={influxDbName}";
        _influxAuthHeader = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{influxUsername}:{influxPassword}"));
        _httpClient = new HttpClient();
        _httpClient.DefaultRequestHeaders.Authorization =
            new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", _influxAuthHeader);
    }

    /// <summary>
    /// 启动同步线程
    /// </summary>
    public void Start()
    {
        _cts = new CancellationTokenSource();
        _workerThread = new Thread(() => RunLoop(_cts.Token))
        {
            Name = "SyncWorker",
            IsBackground = true
        };
        _workerThread.Start();
        Console.WriteLine($"[INFO] 同步线程已启动 (间隔={_syncIntervalSeconds}秒, 回溯={_lookbackMinutes}分钟)");
        Console.WriteLine($"[INFO] InfluxDB 写入地址: {_influxWriteUrl}");
    }

    /// <summary>
    /// 停止同步线程
    /// </summary>
    public void Stop()
    {
        _cts?.Cancel();
        _workerThread?.Join(TimeSpan.FromSeconds(5));
        _httpClient.Dispose();
        Console.WriteLine("[INFO] 同步线程已停止");
    }

    private void RunLoop(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] [SyncWorker] 查询最近 {_lookbackMinutes} 分钟内每个 MMSI 的最新轨迹点...");

                // 1. 读取 MySQL 数据
                var trackPoints = _mysqlService.GetLatestPerMmsiAsync(_lookbackMinutes).GetAwaiter().GetResult().ToList();

                if (trackPoints.Count > 0)
                {
                    Console.WriteLine($"[INFO] [SyncWorker] 获取到 {trackPoints.Count} 条数据（{trackPoints.Count} 个 MMSI）");

                    // 2. 拼接所有数据点为 InfluxDB Line Protocol 格式
                    var dataPoints = new StringBuilder();

                    foreach (var point in trackPoints)
                    {
                        int deivetype = 2;
                        string vehicleNo = point.Mmsi;

                        long gpsTime = point.PositionUtc != 0
                            ? point.PositionUtc
                            : new DateTimeOffset(point.PositionTime).ToUnixTimeSeconds();

                        decimal longitude = point.Lng;
                        decimal latitude = point.Lat;
                        decimal speed = point.Sog != -1 ? point.Sog : 0;
                        decimal state = point.Cog != -1 ? point.Cog : 0;

                        string line = $"trajectoryData,Deivetype={deivetype},vehicle_no={vehicleNo},gps_time={gpsTime} longitude={longitude},latitude={latitude},speed={speed},state={state}\n";
                        dataPoints.Append(line);

                        Console.WriteLine($"  [SyncWorker] MMSI={vehicleNo}, gps_time={gpsTime}, lng={longitude}, lat={latitude}, speed={speed}, state={state}");
                    }

                    // 3. 批量写入 InfluxDB
                    WriteToInfluxDb(dataPoints, trackPoints.Count);

                    Console.WriteLine($"[OK] [SyncWorker] 本轮处理完成，共 {trackPoints.Count} 条");
                }
                else
                {
                    Console.WriteLine("[INFO] [SyncWorker] 暂无新数据");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] [SyncWorker] 同步出错: {ex.Message}");
            }

            // 等待下一轮
            try
            {
                Task.Delay(TimeSpan.FromSeconds(_syncIntervalSeconds), token).GetAwaiter().GetResult();
            }
            catch (TaskCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// 批量写入 InfluxDB
    /// </summary>
    private void WriteToInfluxDb(StringBuilder dataPoints, int dataCount)
    {
        try
        {
            var content = new StringContent(dataPoints.ToString(), Encoding.UTF8, "text/plain");
            var response = _httpClient.PostAsync(_influxWriteUrl, content).GetAwaiter().GetResult();

            if (response.IsSuccessStatusCode)
            {
                string logMsg = $"{DateTime.Now}:总共{dataCount}条数据已成功写入InfluxDB;";
                File.WriteAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RTDataBack.txt"), logMsg);
                Console.WriteLine($"[OK] [SyncWorker] {dataCount} 条数据已成功写入InfluxDB");
            }
            else
            {
                string errorMessage = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                string logMsg = $"{DateTime.Now}:写入InfluxDB时发生错误: {errorMessage};;{dataPoints}";
                File.WriteAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RTDataBack_Error.txt"), logMsg);
                Console.WriteLine($"[ERROR] [SyncWorker] 写入InfluxDB失败: {errorMessage}");
            }
        }
        catch (Exception ex)
        {
            string logMsg = $"{DateTime.Now}发生异常: {ex.Message}";
            File.WriteAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SendInfluxDBError.txt"), logMsg);
            Console.WriteLine($"[ERROR] [SyncWorker] 写入InfluxDB异常: {ex.Message}");
        }
    }
}
