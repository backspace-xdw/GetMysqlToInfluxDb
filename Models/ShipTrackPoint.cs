namespace GetMysqlDataToinfluxDb.Models;

public class ShipTrackPoint
{
    public long Id { get; set; }
    public string Mmsi { get; set; } = "";
    public int DataSource { get; set; }
    public DateTime PositionTime { get; set; }
    public long PositionUtc { get; set; }
    public decimal Lng { get; set; }
    public decimal Lat { get; set; }
    public decimal Sog { get; set; }
    public decimal Cog { get; set; }
    public DateTime CreateTime { get; set; }
}
