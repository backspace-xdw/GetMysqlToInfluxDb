namespace GetMysqlDataToinfluxDb.Models;

/// <summary>
/// 船舶轨迹点实体 - 对应 wits_ship_track_point 表
/// </summary>
public class ShipTrackPoint
{
    public ShipTrackPoint() { }

    public long Id { get; set; }
    public string Mmsi { get; set; } = "";
    public long DataSource { get; set; }
    public DateTime PositionTime { get; set; }
    public long PositionUtc { get; set; }
    public decimal Lng { get; set; }
    public decimal Lat { get; set; }
    public decimal Sog { get; set; }
    public decimal Cog { get; set; }
    public DateTime CreateTime { get; set; }
}
