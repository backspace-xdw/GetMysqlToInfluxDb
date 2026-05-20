namespace GetMysqlDataToinfluxDb.Models;

public class ShipTrackPoint
{
    public long Id { get; set; }
    public string Mmsi { get; set; } = "";
    public int DataSource { get; set; }
    public long LastTimeUtc { get; set; }
    public decimal Lng { get; set; }
    public decimal Lat { get; set; }
    public decimal Sog { get; set; }
    public decimal Cog { get; set; }
    public string ShipName { get; set; } = "";
}
