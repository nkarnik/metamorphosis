package metamorphosis.utils.s3;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import metamorphosis.utils.Utils;
import metamorphosis.utils.s3.FileLockUtil.MultiLock;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.multi.DownloadPackage;
import org.jets3t.service.multi.SimpleThreadedStorageService;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.MultipartUtils;

import com.google.common.io.Files;



public class S3Util {

  
  public static String S3_ACCESS_KEY = "AKIAJWZ2I3PMFF5O6PFA";
  public static String S3_SECRET_KEY = "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";
  private static final AWSCredentials AWS_CREDENTIALS = new AWSCredentials(S3_ACCESS_KEY, S3_SECRET_KEY);
  
  private static Logger _log = Logger.getLogger(S3Util.class);
  public static String addAuthToS3Path(String path) {
    if (path.contains("@")) {
      return path;
    }
    final String newPath = path.replace("s3://", "").replace("s3n://", "");
    return "s3n://" + S3_ACCESS_KEY + ":" + S3_SECRET_KEY + "@" + newPath;
  }
  
  
  public static Pair<String, String> decomposePath(String path) throws S3Exception {
    try {
      URI uri = new URI(path);
      return new Pair<>(uri.getHost(), uri.getPath().replaceAll("^/", ""));
    } catch (URISyntaxException e) {
      throw new S3Exception("Cannot decompose invalid path: " + path);
    }
  }

  
  
  public final static void writeFile(String bucket, String key, byte[] content) throws IOException, S3Exception, InterruptedException {
    
    try {
      RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
      S3Object s3obj = new S3Object(key, content); 
      s3Service.putObject(bucket, s3obj);
    } catch (ServiceException e) {
      throw new S3Exception(e);
    } catch (NoSuchAlgorithmException e) {
      throw new S3Exception(e);
    } catch (IOException e) {
      throw Utils.handleInterruptible(e);
    }
  }
  
  
  public static void writeFile(String bucket, String key, String content) throws IOException, S3Exception, InterruptedException {
    writeFile(bucket, key, content.getBytes(Charset.forName("UTF-8")));
  }
  
  

  public static void writeFileGzip(String bucket, String key, String content) throws IOException, S3Exception, InterruptedException {
  
    // Compress it 
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(buffer);
    gzip.write(content.getBytes(Charset.forName("UTF-8")));
    gzip.close();
    
    // Write it 
    writeFile(bucket, key, buffer.toByteArray());
  
  }
  
  
  public static RestS3Service service() throws S3ServiceException {
    return new RestS3Service(AWS_CREDENTIALS);
  }
  
  
  public static S3Object[] listPath(String bucket, String key) throws S3ServiceException {
    RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
    return s3Service.listObjects(bucket, key, null, 10000);
    
  }
  
  public static void deletePath(String bucket, String key) throws S3ServiceException, S3Exception{
    S3Object[] files = listPath(bucket, key);
    for(S3Object file: files){
      deleteFile(file);
    }
  }
  
  public static String readFile(String bucket, String key) throws IOException {
  
    try {
      RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
      S3Object s3obj = s3Service.getObject(bucket, key);
      BufferedReader reader = new BufferedReader(new InputStreamReader(s3obj.getDataInputStream(), "UTF-8"));
      StringBuilder stringBuilder = new StringBuilder();
      String line; 
      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
        stringBuilder.append('\n');
      }
      return stringBuilder.toString(); 
      
    } catch (ServiceException e) {
      throw new IOException(e);
    }
    
  }
  
  public static byte[] readFileAsBytes(String bucket, String key) throws IOException {
    
    try {
      RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
      S3Object s3obj = s3Service.getObject(bucket, key);
      return IOUtils.toByteArray(s3obj.getDataInputStream());
      
    } catch (ServiceException e) {
      throw new IOException(e);
    }
    
  }



  public static boolean pathExists(String bucket, String path) throws S3Exception {
    try {
      RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
      return s3Service.isObjectInBucket(bucket, path);
    } catch (ServiceException e) {
      throw new S3Exception(e);
    } 
  }

  
  public static S3Object getS3Obj(String bucket, String key) throws IOException{
    try {
      RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
      S3Object s3obj = s3Service.getObject(bucket, key);
      return s3obj;

    } catch (ServiceException e) {
      throw new IOException(e);
    }
  }
  
  
  
  public static Pair<File,BufferedReader> getCachedGzipFileReader(String bucket, String key) throws IOException, InterruptedException, S3Exception {
    return getCachedGzipFileReader(bucket, key, tempDir() + "/s3cache");
  }
  
  public static Pair<File,BufferedReader> getCachedGzipFileReader(String bucket, String key, String root) throws IOException, InterruptedException, S3Exception {
    Pair<File,InputStream> input = getS3ObjInputStreamCached(bucket, key, root);
    return new Pair<File,BufferedReader>(input.getValue0(), new BufferedReader(new InputStreamReader(new GZIPInputStream(input.getValue1()), "UTF-8")));
  }
  
  public static BufferedReader getGzipFileReader(String bucket, String key) throws IOException {
    
    try {
      RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
      S3Object s3obj = s3Service.getObject(bucket, key);
      BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(s3obj.getDataInputStream()), "UTF-8"));
      return reader; 

    } catch (ServiceException e) {
      throw new IOException(e);
    }
    
  }
  
  


  public static String readGzipFile(String bucket, String key) throws IOException {
    
    try {
      RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
      S3Object s3obj = s3Service.getObject(bucket, key);
      BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(s3obj.getDataInputStream()), "UTF-8"));
      StringBuilder stringBuilder = new StringBuilder();
      String line; 
      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
        stringBuilder.append('\n');
      }
      return stringBuilder.toString(); 
      
    } catch (ServiceException e) {
      throw new IOException(e);
    }
    
  }
  
  public static String readGzipFileCached(String bucket, String key) throws IOException, InterruptedException, S3Exception {
    
    Pair<File,InputStream> input = getS3ObjInputStreamCached(bucket, key);
    
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(input.getValue1()), "UTF-8"));
      StringBuilder stringBuilder = new StringBuilder();
      String line; 
      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
        stringBuilder.append('\n');
      }
      return stringBuilder.toString();
    } finally {
      input.getValue1().close();
      input.getValue0().delete();
    }
    
  }
  


  public static String readFile(String settingsFile) throws IOException, S3Exception {
    Pair<String, String> p = decomposePath(settingsFile);
    return readFile(p.getValue0(), p.getValue1());
  }


  public static void writeFile(String file, String contents) throws IOException, S3Exception, InterruptedException {
    Pair<String, String> p = decomposePath(file);
    writeFile(p.getValue0(), p.getValue1(), contents);
  }


  public static String authPath(String bucket, String path) {
    return "s3n://" + S3_ACCESS_KEY + ":" + S3_SECRET_KEY + "@" + bucket + "/" + path.replaceAll("^/", "");
  }


  public static void maybeCreateDirectory(String bucket, String path) throws IOException, S3Exception, InterruptedException {
    final String newPath;
    if (path.matches(".+/$") == false) {
      newPath = path + "/";
    } else {
      newPath = path;
    }
    writeFile(bucket, newPath + "_DIR", "");
  }


  public static InputStream getS3ObjInputStream(String bucket, String key) throws ServiceException {
    RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
    S3Object s3obj = s3Service.getObject(bucket, key);
    return s3obj.getDataInputStream();
  }
  
  
  
  public static String tempDir() {
      return "/tmp";
  }
  
  
  public static Pair<File,InputStream> getS3ObjInputStreamCached(String bucket, String key) throws InterruptedException, S3Exception {
    return getS3ObjInputStreamCached(bucket, key, tempDir() + "/s3cache");
  }
  
  
  public static void removeCachedFiles(String bucket, String key, String directory) throws IOException {
    final String dirPath = directory + "/" + key;
    org.apache.commons.io.FileUtils.deleteDirectory(new File(dirPath));
  }
  
  public static Pair<File,InputStream> getS3ObjInputStreamCached(String bucket, String key, String directory) throws InterruptedException, S3Exception {
    
    // Init 
    final String dirPath = directory + "/" + key;
    final String filePath = dirPath + "/content";
    final String readyPath = dirPath + "/READY";
    final String lockPath = dirPath + "/LOCK";
    do {
      
      // Can we read this file? 
      File readyFile = new File(readyPath);
      File file = new File(filePath);
      if (readyFile.exists() && file.exists()) {
        try {
          return new Pair<File,InputStream>(file, new FileInputStream(file));
        } catch (FileNotFoundException e) {
          throw new S3Exception(e);
        }
      }
      
      // Let's try to get a lock...
      try (final MultiLock lock = FileLockUtil.lock(lockPath)) {
 
        _log.info("Caching content: " + key);
        final RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
        final S3Object s3obj = s3Service.getObject(bucket, key);
        final DownloadPackage dl = new DownloadPackage(s3obj, file);
        final SimpleThreadedStorageService simpleMulti = new SimpleThreadedStorageService(s3Service);
        simpleMulti.downloadObjects(bucket, new DownloadPackage[] {dl});
        //try (final OutputStream out = new FileOutputStream(file)) {
        //  IOUtils.copy(s3obj.getDataInputStream(), out);
        //}
 
        // File is copied. Create the READY file and we're done
        Files.touch(readyFile);
 
        // Release
        _log.info("Caching content done. File: " + readyPath);
      } catch (IOException e) {
        throw new S3Exception(Utils.handleInterruptible(e));
      } catch(ServiceException e) {
        throw new S3Exception(e);
      }
    
    } while (true);

  }


  public static String cachedReadFile(String bucket, String key) throws InterruptedException, S3Exception  {
    Pair<File, InputStream> s3ObjInputStreamCached = getS3ObjInputStreamCached(bucket, key);
    try {
      BufferedReader reader = new BufferedReader( new InputStreamReader( s3ObjInputStreamCached.getValue1() ) );
      StringBuilder stringBuilder = new StringBuilder();
      String line; 
      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
        stringBuilder.append('\n');
      }
      return stringBuilder.toString();
    } catch(IOException e) {
      throw new S3Exception(e);
    } finally {
      try {
        s3ObjInputStreamCached.getValue1().close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      s3ObjInputStreamCached.getValue0().delete();
    }

  }
  public static void main(String[] args) throws S3Exception {
    System.out.println("Running");
    String cmd = "ZILLABYTE_API_KEY=__zilla_web_of_trust__643eb89103d9490fb3cbc98c06f87dea7e6df97e4ab33cee1221f0f0169cae362305879837b841ef5f2ecab1381db72e0259 ZILLABYTE_API_HOST=192.168.111.1 ZILLABYTE_API_PORT=5000 zillabyte query:pull:s3 \"SELECT  *   FROM  r0012__web_pages  \" AKIAJWZ2I3PMFF5O6PFA F9rmZ36zlk2rNNRunsbYQh53+OF6rPdzy6HtI6bf files.zillabyte.com dev-sashi/queries/8c686b0c65f8e2ef69542e3f2395ee79_1394765575094";
    ProcessBuilder pb = new ProcessBuilder("/bin/bash","-c","-l", cmd);
    try {
      pb.start().waitFor();
    } catch (InterruptedException e) {
      throw new S3Exception("Unload to s3 was interrupted", e);
    } catch (IOException e) {
      throw new S3Exception("Unload to s3 had an IOException", e);
    }
    
//    String _bucket = "files.zillabyte.com";j
//    String _key = "test/queries/sashi_test/0000_part_00.gz";
//    
//    BufferedReader cachedGzipFileReader = S3Util.getCachedGzipFileReader(_bucket, _key);
//    String line;
//    
//    while((line = cachedGzipFileReader.readLine()) != null){
//      
//      String[] values = line.split("\",\""); //Contains field values and metatuple columns
//      //Remove quotes on first and last values.
//      values[0] = values[0].substring(1);
//      String last = values[values.length - 1];
//      values[values.length - 1] = last.substring(0, last.length() - 1);
//
//      System.out.println(values.length + " Values: " + Joiner.on(",").join(values));
//      
//    }

  }


  public static void copyFile(File localFile, String bucket, String path) throws S3Exception {
    RestS3Service s3Service;
    try {
      s3Service = new RestS3Service(AWS_CREDENTIALS);
    } catch (S3ServiceException e) {
      throw new S3Exception(e);
    }
    
    List<StorageObject> objectsToUploadAsMultipart = new ArrayList<>();
    S3Object s3obj;
    try {
      s3obj = new S3Object(localFile);
    } catch (NoSuchAlgorithmException e) {
      throw new S3Exception(e);
    } catch (IOException e) {
      throw new S3Exception(e);
    }
    s3obj.setKey(path);
    objectsToUploadAsMultipart.add(s3obj);

    long maxSizeForAPartInBytes = 5 * 1024 * 1024;
    MultipartUtils mpUtils = new MultipartUtils(maxSizeForAPartInBytes);

    try {
      mpUtils.uploadObjects(bucket, s3Service, objectsToUploadAsMultipart, null);
    } catch (Exception e) {
      // Unfortunately we *must* catch Exception here since that is the signature of mpUtils.uploadObjects.
      throw new S3Exception(e);
    }
  }


  public static void deleteFile(String bucket, String path) throws S3Exception {
    try {
      final RestS3Service s3Service = new RestS3Service(AWS_CREDENTIALS);
      s3Service.deleteObject(bucket, path);
    } catch (ServiceException e) {
      throw new S3Exception(e);
    }
  }
  
  public static void deleteFile(S3Object o) throws S3Exception {
    deleteFile(o.getBucketName(), o.getKey());
  }





}
