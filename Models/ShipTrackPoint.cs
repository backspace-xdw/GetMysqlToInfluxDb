namespace GetMysqlDataToinfluxDb.Models;

/// <summary>
/// 船舶轨迹点实体 - 对应 wits_ship_track_point 表
/// </summary>
public class ShipTrackPoint
{
    public long Id { get; set; }

    /// <summary>船舶 MMSI</summary>
    public string Mmsi { get; set; } = string.Empty;

    /// <summary>数据来源（0=岸基/船基AIS基站，1=卫星基站）</summary>
    public sbyte DataSource { get; set; }

    /// <summary>位置时间（北京时间）</summary>
    public DateTime PositionTime { get; set; }

    /// <summary>位置时间戳（秒）</summary>
    public long PositionUtc { get; set; }

    /// <summary>经度（WGS84坐标系）</summary>
    public decimal Lng { get; set; }

    /// <summary>纬度（WGS84坐标系）</summary>
    public decimal Lat { get; set; }

    /// <summary>航速 SOG（节，-1为无效）</summary>
    public decimal Sog { get; set; }

    /// <summary>航向 COG（度，-1为无效）</summary>
    public decimal Cog { get; set; }

    /// <summary>入库时间</summary>
    public DateTime CreateTime { get; set; }
}
