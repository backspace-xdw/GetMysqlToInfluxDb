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
                IFNULL(t.last_time_utc, 0) AS last_time_utc,
                IFNULL(t.lng, 0) AS lng,
                IFNULL(t.lat, 0) AS lat,
                IFNULL(t.sog, 0) AS sog,
                IFNULL(t.cog, 0) AS cog,
                IFNULL(t.ship_cnname, '') AS ship_name
            FROM wits_shipxy_area_ship_snapshot t
            INNER JOIN (
                SELECT MAX(id) AS max_id
                FROM wits_shipxy_area_ship_snapshot
                WHERE mmsi IS NOT NULL AND mmsi <> ''
                  AND lng IS NOT NULL AND lat IS NOT NULL
                  AND last_time_utc >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL {lookbackMinutes} MINUTE))
                GROUP BY mmsi
            ) latest ON t.id = latest.max_id";

        using var cmd = new MySqlCommand(sql, connection) { CommandTimeout = 30 };
        using var reader = await cmd.ExecuteReaderAsync();

        var list = new List<ShipTrackPoint>();
        while (await reader.ReadAsync())
        {
            list.Add(new ShipTrackPoint
            {
                Id = reader.GetInt64("id"),
                Mmsi = reader.GetString("mmsi"),
                DataSource = reader.GetInt32("data_source"),
                LastTimeUtc = reader.GetInt64("last_time_utc"),
                Lng = reader.GetDecimal("lng"),
                Lat = reader.GetDecimal("lat"),
                Sog = reader.GetDecimal("sog"),
                Cog = reader.GetDecimal("cog"),
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
