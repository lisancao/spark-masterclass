package book;

/** One trip as a plain JVM object, for the serializer comparison in Chapter 42. */
public class Trip implements java.io.Serializable {
  public long id;
  public int zone;
  public double distance;
  public String vendor;

  public Trip() {}                      // Kryo's default instantiator uses the no-argument constructor

  public Trip(long id, int zone, double distance, String vendor) {
    this.id = id; this.zone = zone; this.distance = distance; this.vendor = vendor;
  }
}
