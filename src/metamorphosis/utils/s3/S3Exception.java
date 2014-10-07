package metamorphosis.utils.s3;


public class S3Exception extends Exception {
  
  /**
   * 
   */
  private static final long serialVersionUID = 9024346679823939304L;

  public S3Exception(Throwable ex) {
    super(ex);
  }

  public S3Exception(String s, Throwable ex) {
    super(s, ex);
  }

  public S3Exception(String string) {
    super(string);
  }
}
