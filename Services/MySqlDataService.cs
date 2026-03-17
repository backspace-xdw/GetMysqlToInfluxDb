using Dapper;
using MySqlConnector;
using GetMysqlDataToinfluxDb.Models;

namespace GetMysqlDataToinfluxDb.Services;

/// <summary>
/// MySQL 数据读取服务
/// </summary>
public class MySqlDataService
{
    private readonly string _connectionString;

    public MySqlDataService(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// 获取当前时间前 N 分钟内的数据，每个 mmsi 只取 position_time 最新的一条
    /// </summary>
    public async Task<IEnumerable<ShipTrackPoint>> GetLatestPerMmsiAsync(int lookbackMinutes)
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        // 直接拼接整数值，避免 MySqlConnector 参数化与用户变量冲突
        var sql = $@"
            SELECT
                t.id AS Id,
                t.mmsi AS Mmsi,
                CAST(IFNULL(t.data_source, 0) AS SIGNED) AS DataSource,
                t.position_time AS PositionTime,
                IFNULL(t.position_utc, 0) AS PositionUtc,
                t.lng AS Lng,
                t.lat AS Lat,
                IFNULL(t.sog, 0) AS Sog,
                IFNULL(t.cog, 0) AS Cog,
                IFNULL(t.create_time, t.position_time) AS CreateTime
            FROM wits_ship_track_point t
            INNER JOIN (
                SELECT mmsi, MAX(position_time) AS max_time
                FROM wits_ship_track_point
                WHERE position_time >= DATE_SUB(NOW(), INTERVAL {lookbackMinutes} MINUTE)
                GROUP BY mmsi
            ) latest ON t.mmsi = latest.mmsi AND t.position_time = latest.max_time";

        return await connection.QueryAsync<ShipTrackPoint>(sql);
    }

    /// <summary>
    /// 测试数据库连接
    /// </summary>
    public async Task<bool> TestConnectionAsync()
    {
        try
        {
            using var connection = new MySqlConnection(_connectionString);
            await connection.OpenAsync();
            Console.WriteLine("[OK] MySQL 连接成功");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] MySQL 连接失败: {ex.Message}");
            return false;
        }
    }
}
