#nullable disable
namespace GetMysqlDataToinfluxDb.Models;

public class ShipTrackPoint
{
    public ShipTrackPoint() { }

    public ShipTrackPoint(long id, string mmsi, long dataSource, DateTime positionTime,
        long positionUtc, decimal lng, decimal lat, decimal sog, decimal cog, DateTime createTime)
    {
        Id = id;
        Mmsi = mmsi;
        DataSource = dataSource;
        PositionTime = positionTime;
        PositionUtc = positionUtc;
        Lng = lng;
        Lat = lat;
        Sog = sog;
        Cog = cog;
        CreateTime = createTime;
    }

    public long Id { get; set; }
    public string Mmsi { get; set; }
    public long DataSource { get; set; }
    public DateTime PositionTime { get; set; }
    public long PositionUtc { get; set; }
    public decimal Lng { get; set; }
    public decimal Lat { get; set; }
    public decimal Sog { get; set; }
    public decimal Cog { get; set; }
    public DateTime CreateTime { get; set; }
}
