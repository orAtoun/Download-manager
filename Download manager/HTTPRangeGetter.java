import java.io.DataInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

/**
 * A runnable class which downloads a given url.
 * It reads CHUNK_SIZE at a time and writs it into a BlockingQueue.
 * It supports downloading a range of data, and limiting the download rate using a token bucket.
 */
public class HTTPRangeGetter implements Runnable {
    static final int CHUNK_SIZE = 4096;
    private static final int CONNECT_TIMEOUT = 500;
    private static final int READ_TIMEOUT = 2000;
    private final String url;
    private final Range range;
    private final BlockingQueue<Chunk> outQueue;
    private TokenBucket tokenBucket;
    private DownloadableMetadata metaData;

    HTTPRangeGetter(
            String url,
            Range range,
            BlockingQueue<Chunk> outQueue,
            TokenBucket tokenBucket, 
            DownloadableMetadata metaData) {
        this.url = url;
        this.range = range;
        this.outQueue = outQueue;
        this.tokenBucket = tokenBucket;
        this.metaData = metaData;
    }

	private void downloadRange() throws InterruptedException, IOException {
		int numOfChunksToWorker = (int) Math.ceil(this.range.getLength().intValue() / (CHUNK_SIZE + 0.0));
		int chunksThatWasDelivered = 0;
		HttpURLConnection connection = null;
		DataInputStream dataInputStream = null;
		try {
	    	connection = (HttpURLConnection) new URL(this.url).openConnection();
	    	connection.setRequestMethod("GET");
	    	connection.setConnectTimeout(CONNECT_TIMEOUT);
	    	connection.setReadTimeout(READ_TIMEOUT);
	    	connection.setRequestProperty("Range", "bytes=" + this.range.getStart() + "-" + this.range.getEnd());
	    	connection.connect();
	    	dataInputStream = new DataInputStream(connection.getInputStream());
	    	while(chunksThatWasDelivered < numOfChunksToWorker) {
	        	int size = CHUNK_SIZE;
	        	if(chunksThatWasDelivered == numOfChunksToWorker - 1) {
	        		size = (int) this.range.getLength().longValue() - chunksThatWasDelivered * CHUNK_SIZE;
	        	}
	        	byte[] data = new byte[size];
	        	this.tokenBucket.take(size);
	        	dataInputStream.readFully(data, 0, size);
	        	this.outQueue.put(new Chunk(data, this.range.getStart() + chunksThatWasDelivered * CHUNK_SIZE, size));
	        	chunksThatWasDelivered++;
	    	}
		} finally {
			if(dataInputStream != null) {
		    	dataInputStream.close();
			}
			if(connection != null) {
		    	connection.disconnect();
			}
		}
    }

    @Override
    public void run() {
        try {
            this.downloadRange();
        } catch (IOException | InterruptedException e) {
        	this.metaData.changeRangeToNotDownloaded(this.range);
        }
    }
}
