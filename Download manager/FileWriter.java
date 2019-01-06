import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;

/**
 * This class takes chunks from the queue, writes them to disk and updates the file's metadata.
 *
 * NOTE: make sure that the file interface you choose writes every update to the file's content or metadata
 *       synchronously to the underlying storage device.
 */
public class FileWriter implements Runnable {

    private final BlockingQueue<Chunk> chunkQueue;
    private DownloadableMetadata downloadableMetadata;

    FileWriter(DownloadableMetadata downloadableMetadata, BlockingQueue<Chunk> chunkQueue) {
        this.chunkQueue = chunkQueue;
        this.downloadableMetadata = downloadableMetadata;
    }

    private void writeChunks() throws InterruptedException, IOException {
		File outputFile = new File("./" + this.downloadableMetadata.getFilename());
		RandomAccessFile randomAccessFileOutput = new RandomAccessFile(outputFile, "rws");
		int precentageDownloaded = 0;
        while(!this.downloadableMetadata.isCompleted()) {
			Chunk availableChunk = this.chunkQueue.take();
        	try {
				randomAccessFileOutput.seek(availableChunk.getOffset());
				randomAccessFileOutput.write(availableChunk.getData(), 0, availableChunk.getSize_in_bytes());
				this.downloadableMetadata.addRange(new Range(availableChunk.getOffset(), availableChunk.getOffset() + availableChunk.getSize_in_bytes() - 1));
				this.downloadableMetadata.saveMetadataFile();
				int currentPrecentage = this.downloadableMetadata.getInPrecentageHowMuchDownloaded();
				if (currentPrecentage > precentageDownloaded) {
        			precentageDownloaded = currentPrecentage;
        			System.err.println("Downloaded " + precentageDownloaded + "%");
				}
    		} catch (IOException e) {
    			this.downloadableMetadata.changeRangeToNotDownloaded(new Range(availableChunk.getOffset(), availableChunk.getOffset() + availableChunk.getSize_in_bytes() - 1));
    			if(randomAccessFileOutput != null) {
        			randomAccessFileOutput.close();
    			}
    			System.err.println("Failed to write to the file");
    			System.err.println("Download failed");
    			System.exit(1); // 1 represents status code of failure
    		}
        }
        if (randomAccessFileOutput != null) {
        	randomAccessFileOutput.close();
        }
    }

    @Override
    public void run() {
        try {
            this.writeChunks();
        } catch (InterruptedException | IOException e) {
			System.err.println("Failed to write to the file");
			System.err.println("Download failed");
			System.exit(1); // 1 represents status code of failure
        }
    }
}
