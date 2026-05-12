using MySqlConnector;
using GetMysqlDataToinfluxDb.Models;

namespace GetMysqlDataToinfluxDb.Services;

public class MySqlDataService
{
    private readonly string _connectionString;

    public MySqlDataService(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<List<ShipTrackPoint>> GetLatestPerMmsiAsync(int lookbackMinutes)
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        var sql = $@"
            SELECT
                t.id,
                t.mmsi,
                IFNULL(t.data_source, 0) AS data_source,
                t.position_time,
                IFNULL(t.position_utc, 0) AS position_utc,
                t.lng,
                t.lat,
                IFNULL(t.sog, 0) AS sog,
                IFNULL(t.cog, 0) AS cog,
                IFNULL(t.create_time, t.position_time) AS create_time,
                IFNULL(ct.ship_name, '') AS ship_name
            FROM wits_ship_track_point t
            INNER JOIN (
                SELECT mmsi, MAX(position_time) AS max_time
                FROM wits_ship_track_point
                WHERE position_time >= DATE_SUB(NOW(), INTERVAL {lookbackMinutes} MINUTE)
                GROUP BY mmsi
            ) latest ON t.mmsi = latest.mmsi AND t.position_time = latest.max_time
            LEFT JOIN (
                SELECT mmsi, ship_name
                FROM wits_checkpoint_transit ct1
                WHERE ship_name IS NOT NULL AND ship_name <> ''
                  AND mmsi IS NOT NULL AND mmsi <> ''
                  AND id = (
                      SELECT MAX(id) FROM wits_checkpoint_transit ct2
                      WHERE ct2.mmsi = ct1.mmsi
                        AND ct2.ship_name IS NOT NULL AND ct2.ship_name <> ''
                  )
            ) ct ON t.mmsi = ct.mmsi";

        using var cmd = new MySqlCommand(sql, connection);
        using var reader = await cmd.ExecuteReaderAsync();

        var list = new List<ShipTrackPoint>();
        while (await reader.ReadAsync())
        {
            list.Add(new ShipTrackPoint
            {
                Id = reader.GetInt64("id"),
                Mmsi = reader.GetString("mmsi"),
                DataSource = reader.GetInt32("data_source"),
                PositionTime = reader.GetDateTime("position_time"),
                PositionUtc = reader.GetInt64("position_utc"),
                Lng = reader.GetDecimal("lng"),
                Lat = reader.GetDecimal("lat"),
                Sog = reader.GetDecimal("sog"),
                Cog = reader.GetDecimal("cog"),
                CreateTime = reader.GetDateTime("create_time"),
                ShipName = reader.GetString("ship_name")
            });
        }

        return list;
    }

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
