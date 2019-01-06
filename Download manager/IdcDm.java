import java.io.IOException;
import java.util.concurrent.*;

public class IdcDm {

    /**
     * Receive arguments from the command-line, provide some feedback and start the download.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        int numberOfWorkers = 1;
        Long maxBytesPerSecond = null;

        if (args.length < 1 || args.length > 3) {
            System.err.printf("usage:\n\tjava IdcDm URL [MAX-CONCURRENT-CONNECTIONS] [MAX-DOWNLOAD-LIMIT]\n");
            System.exit(1);
        } else if (args.length >= 2) {
            numberOfWorkers = Integer.parseInt(args[1]);
            if (args.length == 3)
                maxBytesPerSecond = Long.parseLong(args[2]);
        }

        String url = args[0];

        System.err.printf("Downloading");
        if (numberOfWorkers > 1)
            System.err.printf(" using %d connections", numberOfWorkers);
        if (maxBytesPerSecond != null)
            System.err.printf(" limited to %d Bps", maxBytesPerSecond);
        System.err.printf("...\n");

        DownloadURL(url, numberOfWorkers, maxBytesPerSecond);
    }

    /**
     * Initiate the file's metadata, and iterate over missing ranges. For each:
     * 1. Setup the Queue, TokenBucket, DownloadableMetadata, FileWriter, RateLimiter, and a pool of HTTPRangeGetters
     * 2. Join the HTTPRangeGetters, send finish marker to the Queue and terminate the TokenBucket
     * 3. Join the FileWriter and RateLimiter
     *
     * Finally, print "Download succeeded/failed" and delete the metadata as needed.
     *
     * @param url URL to download
     * @param numberOfWorkers number of concurrent connections
     * @param maxBytesPerSecond limit on download bytes-per-second
     */
    private static void DownloadURL(String url, int numberOfWorkers, Long maxBytesPerSecond) {
    	boolean downloadSuccess = true;
    	DownloadableMetadata metaData = null;
    	try {
        	metaData = new DownloadableMetadata(url);
    	} catch (IOException e) {
    		downloadSuccess = false;
    		System.err.println("Failed to create metaData object");
    	}
    	if (metaData != null && downloadSuccess) {
    		BlockingQueue<Chunk> chunkQueue = new ArrayBlockingQueue<>(numberOfWorkers);
        	TokenBucket tokenBucket = new TokenBucket();
            Thread threadRateLimiter = new Thread(new RateLimiter(tokenBucket, maxBytesPerSecond));
            threadRateLimiter.start();
        	Thread threadFileWriter = new Thread(new FileWriter(metaData, chunkQueue));
        	threadFileWriter.start();
        	Thread[] pollOfThreads = new Thread[numberOfWorkers];
        	while(!metaData.isCompleted()) {
        		metaData.setNumOfMaxChunksToWorker(numberOfWorkers);
            	for(int i = 0; i < numberOfWorkers; i++) {
            		Range missinRange = metaData.getMissingRange();
            		if (missinRange != null) {
            			pollOfThreads[i] = new Thread(new HTTPRangeGetter(url, missinRange, chunkQueue, tokenBucket, metaData));
            		}
            	}
            	for(int i = 0; i < numberOfWorkers; i++) {
            		if (pollOfThreads[i] != null && pollOfThreads[i].getState() == Thread.State.NEW) {
            			pollOfThreads[i].start();
            		}
            	}
        		try {
                	for(int i = 0; i < numberOfWorkers; i++) {
                		if (pollOfThreads[i] != null) {
                			pollOfThreads[i].join();
                		}
                	}
    			} catch (InterruptedException e) {
    				System.err.println("Failed to wait to one of the threads");
    				downloadSuccess = false;
    			}
        	}
        	tokenBucket.terminate();
        	try {
            	threadFileWriter.join();
            	threadRateLimiter.join();
        	} catch (InterruptedException e) {
    			System.err.println("Failed to wait to one of the threads");
    			downloadSuccess = false;
    		}
    	}
    	if (downloadSuccess) {
    		metaData.delete();
    		System.err.println("Download succeeded");
    	} else {
    		System.err.println("Download failed");
    	}
    }
}
