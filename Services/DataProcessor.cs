using GetMysqlDataToinfluxDb.Models;

namespace GetMysqlDataToinfluxDb.Services;

/// <summary>
/// 数据处理服务
/// 将 MySQL 船舶轨迹数据解析为 InfluxDB trajectoryData 格式字段
/// </summary>
public class DataProcessor
{
    /// <summary>
    /// 处理一批船舶轨迹点数据
    /// </summary>
    public async Task ProcessBatchAsync(IEnumerable<ShipTrackPoint> trackPoints)
    {
        var points = trackPoints.ToList();
        if (points.Count == 0)
        {
            Console.WriteLine("[INFO] 本批次无数据需要处理");
            return;
        }

        Console.WriteLine($"[INFO] 开始处理 {points.Count} 条轨迹点数据...");

        foreach (var point in points)
        {
            // ===== 字段映射: MySQL → InfluxDB trajectoryData =====
            // measurement 固定为 trajectoryData
            string measurement = "trajectoryData";

            // Deivetype 固定为 2
            int deivetype = 2;

            // vehicle_no = 船舶 MMSI
            string vehicleNo = point.Mmsi;

            // gps_time = position_utc 秒级时间戳 → 毫秒级时间戳
            // 如果 position_utc 为空则用 position_time 转换
            long gpsTime = point.PositionUtc.HasValue
                ? point.PositionUtc.Value * 1000
                : new DateTimeOffset(point.PositionTime).ToUnixTimeMilliseconds();

            // longitude = lng 经度
            decimal longitude = point.Lng;

            // latitude = lat 纬度
            decimal latitude = point.Lat;

            // speed = sog 航速（无效值 -1 设为 0）
            decimal speed = (point.Sog.HasValue && point.Sog.Value != -1) ? point.Sog.Value : 0;

            // state = cog 航向（无效值 -1 设为 0）
            decimal state = (point.Cog.HasValue && point.Cog.Value != -1) ? point.Cog.Value : 0;

            Console.WriteLine($"  MMSI={vehicleNo}, gps_time={gpsTime}, lng={longitude}, lat={latitude}, speed={speed}, state={state}");

            // ===== 预留: 写入 InfluxDB =====
            // TODO: 使用上面解析好的变量写入 InfluxDB
            // 格式: trajectoryData, Deivetype=2,vehicle_no={vehicleNo},gps_time={gpsTime},longitude={longitude},latitude={latitude},speed={speed},state={state}
            await WriteToInfluxDbAsync(measurement, deivetype, vehicleNo, gpsTime, longitude, latitude, speed, state);
        }

        Console.WriteLine($"[OK] 成功处理 {points.Count} 条数据");
    }

    /// <summary>
    /// 预留: 写入 InfluxDB
    /// </summary>
    private async Task WriteToInfluxDbAsync(string measurement, int deivetype, string vehicleNo,
        long gpsTime, decimal longitude, decimal latitude, decimal speed, decimal state)
    {
        // TODO: 实现 InfluxDB 写入逻辑
        // 各字段已解析就绪，可直接使用
        await Task.CompletedTask;
    }
}
