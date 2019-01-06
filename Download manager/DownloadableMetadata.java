import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Describes a file's metadata: URL, file name, size, and which parts already downloaded to disk.
 *
 * The metadata (or at least which parts already downloaded to disk) is constantly stored safely in disk.
 * When constructing a new metadata object, we first check the disk to load existing metadata.
 *
 * CHALLENGE: try to avoid metadata disk footprint of O(n) in the average case
 * HINT: avoid the obvious bitmap solution, and think about ranges...
 */
class DownloadableMetadata {
	static final byte CHUNK_WAS_NOT_DOWNLOADED = 0;
	static final byte CHUNK_WAS_DOWNLOADED_ALREADY = 1;
	static final byte CHUNK_IN_PROCESS_OF_DOWNLOAD = 2;
	
    private final String metadataFilename;
    private String filename;
    private String url;
    private byte[] downladableParts;
    private AtomicInteger sizeWasDownloaded;
    private int fullSize;
    private int numOfMaxChunksToWorker;

    DownloadableMetadata(String url) throws IOException {
        this.url = url;
        this.filename = getName(url);
        this.metadataFilename = getMetadataName(filename);
        File metaData = new File(this.metadataFilename);
        boolean successToReproduceMetaData = false;
        if(!metaData.createNewFile()) {
        	while(!successToReproduceMetaData) {
            	try {
            		reproduceMetaDataFile(metaData);
            		successToReproduceMetaData = true;
            	} catch (ClassNotFoundException | IOException e) {
            		metaData = new File(this.filename + ".tmp");
            	}
        	}
        } else {
        	this.sizeWasDownloaded = new AtomicInteger(0);
        	this.fullSize = getSizeOfFile();
        	// Split the all file to chunks. Chunk size is the basic size that each worker works with.
        	this.downladableParts = new byte[(int) Math.ceil(this.fullSize / (HTTPRangeGetter.CHUNK_SIZE + 0.0))];
        	initilizeDownladableParts();
        }
    }
    
    private void initilizeDownladableParts() {
    	for(int i = 0; i < this.downladableParts.length; i++) {
    		this.downladableParts[i] = CHUNK_WAS_NOT_DOWNLOADED;
    	}
    }
    
    public void setNumOfMaxChunksToWorker(int numberOfWorkers) {
    	this.numOfMaxChunksToWorker = (int) Math.ceil((this.fullSize - this.sizeWasDownloaded.get()) / (HTTPRangeGetter.CHUNK_SIZE + 0.0) / (numberOfWorkers + 0.0));
    }
    
    private void reproduceMetaDataFile(File metaData) throws IOException, ClassNotFoundException {
    	ObjectInputStream objInputStream = new ObjectInputStream(new FileInputStream(metaData));
    	this.fullSize = getSizeOfFile();
    	this.downladableParts = getCorrectArray((byte[])objInputStream.readObject());
   		objInputStream.close();
    }
    
    private int getSizeOfFile() throws IOException {
       	HttpURLConnection connection = (HttpURLConnection) new URL(this.url).openConnection();
    	connection.setRequestMethod("HEAD");
    	connection.connect();
    	int sizeOfFile = connection.getContentLength();
    	connection.disconnect();
    	return sizeOfFile;
    }
    
    private byte[] getCorrectArray(byte[] array) {
    	this.sizeWasDownloaded = new AtomicInteger(0);
    	for(int i = 0; i < array.length; i++) {
    		if (array[i] == CHUNK_IN_PROCESS_OF_DOWNLOAD) {
    			array[i] = CHUNK_WAS_NOT_DOWNLOADED;
    		} else if (array[i] == CHUNK_WAS_DOWNLOADED_ALREADY) {
    			if(i == array.length - 1) {
    				int sizeOfLastChunk = this.fullSize - (array.length - 1) * HTTPRangeGetter.CHUNK_SIZE;
    				this.sizeWasDownloaded.addAndGet(sizeOfLastChunk);
    			} else {
        			this.sizeWasDownloaded.addAndGet(HTTPRangeGetter.CHUNK_SIZE);
    			}
    		}
    	}
    	return array;
    }
    
    public void saveMetadataFile() throws IOException {
    	File metaData = new File(this.metadataFilename);
    	File tempFile = new File(this.filename + ".tmp");
    	if(metaData.exists()) {
        	copyMetaData(metaData, tempFile);
    	}
        ObjectOutputStream objOutputStream = new ObjectOutputStream(new FileOutputStream(this.metadataFilename));
        objOutputStream.writeObject(this.downladableParts);
        objOutputStream.close();
        tempFile.delete();
    }
    
    private void copyMetaData(File metaData, File tempFile) throws IOException {
    	InputStream inputStream = null;
    	OutputStream outputStream = null;
    	try {
    		inputStream = new BufferedInputStream(new FileInputStream(metaData));
    		outputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
    		byte[] data = new byte[1024];
    		int size = inputStream.read(data);
    		while (size > 0) {
    			outputStream.write(data, 0, size);
    			size = inputStream.read(data);
    		}
    	} finally {
    		if(inputStream != null) {
        		inputStream.close();
    		}
    		if(outputStream != null) {
        		outputStream.close();
    		}
    	}
    }

    private static String getMetadataName(String filename) {
        return filename + ".metadata";
    }

    private static String getName(String path) {
        return path.substring(path.lastIndexOf('/') + 1, path.length());
    }

    void addRange(Range range) {
    	int chunkSize = HTTPRangeGetter.CHUNK_SIZE;
        long amountOfChunks = (long) Math.ceil(range.getLength() / (chunkSize + 0.0));
        for(int i = 0; i < amountOfChunks; i++) {
        	this.downladableParts[(int) ((range.getStart() / chunkSize) + i)] = CHUNK_WAS_DOWNLOADED_ALREADY;
        	if(i == amountOfChunks - 1) {
        		int addToWasDownloaded = (int) range.getLength().longValue() - chunkSize * i;
        		this.sizeWasDownloaded.addAndGet(addToWasDownloaded);
        	} else {
            	this.sizeWasDownloaded.addAndGet(chunkSize);
        	}
        }
    }
    
    void changeRangeToNotDownloaded(Range range) {
    	int numOfChunks = (int) Math.ceil(range.getLength().intValue() / (HTTPRangeGetter.CHUNK_SIZE + 0.0));
    	int indexStart = (int) (range.getStart() / HTTPRangeGetter.CHUNK_SIZE);
    	for(int i = 0; i < numOfChunks; i++) {
    		if(this.downladableParts[indexStart + i] == CHUNK_IN_PROCESS_OF_DOWNLOAD) {
        		this.downladableParts[indexStart + i] = CHUNK_WAS_NOT_DOWNLOADED;
    		}
    	}
    }

    String getFilename() {
        return filename;
    }
    
    boolean isCompleted() {
    	return this.fullSize <= this.sizeWasDownloaded.get();
    }

    void delete() {
        File metaData = new File("./" + this.metadataFilename);
        metaData.delete();
        File tempFile = new File(this.filename + ".tmp");
        if(tempFile.exists()) {
        	tempFile.delete();
        }
    }
    
    Range getMissingRange() {
    	Long startRange = null;
    	Long endRange = null;
    	int numOfChunkToWorker = 0;
    	boolean startCounting = false;
        for(int i = 0; i < this.downladableParts.length; i++) {
        	if(startCounting && (this.downladableParts[i] == CHUNK_IN_PROCESS_OF_DOWNLOAD || this.downladableParts[i] == CHUNK_WAS_DOWNLOADED_ALREADY)) {
        		endRange = startRange + numOfChunkToWorker;
        		return new Range(startRange * HTTPRangeGetter.CHUNK_SIZE, endRange * HTTPRangeGetter.CHUNK_SIZE - 1);
        	}
        	if (numOfChunkToWorker == this.numOfMaxChunksToWorker) {
        		endRange = startRange + numOfChunkToWorker;
        		return new Range(startRange * HTTPRangeGetter.CHUNK_SIZE, endRange * HTTPRangeGetter.CHUNK_SIZE - 1);
        	}
        	if(this.downladableParts[i] == CHUNK_WAS_NOT_DOWNLOADED) {
        		this.downladableParts[i] = CHUNK_IN_PROCESS_OF_DOWNLOAD;
        		if(!startCounting) {
        			startRange = (long) i;
        			startCounting = true;
        		}
        		numOfChunkToWorker++;
        	}
        }
        if(startRange != null) {
    		endRange = (long) this.fullSize - 1;
    		return new Range(startRange * HTTPRangeGetter.CHUNK_SIZE, endRange);
        }
        return null;
    }

    String getUrl() {
        return url;
    }
    
    byte[] getDownladableParts() {
    	return this.downladableParts;
    }
    
    AtomicInteger getSizeWasDownloaded() {
    	return this.sizeWasDownloaded;
    }
    
    int getFullSize() {
    	return this.fullSize;
    }
    
    int getInPrecentageHowMuchDownloaded() {
    	return (int) ((this.sizeWasDownloaded.get() / (this.fullSize + 0.0)) * 100);
    }
}
