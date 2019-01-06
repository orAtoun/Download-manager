/**
 * A token bucket based rate-limiter.
 *
 * This class should implement a "soft" rate limiter by adding maxBytesPerSecond tokens to the bucket every second,
 * or a "hard" rate limiter by resetting the bucket to maxBytesPerSecond tokens every second.
 */
public class RateLimiter implements Runnable {
    private final TokenBucket tokenBucket;
    private final Long maxBytesPerSecond;

    RateLimiter(TokenBucket tokenBucket, Long maxBytesPerSecond) {
        this.tokenBucket = tokenBucket;
        this.maxBytesPerSecond = maxBytesPerSecond;
    }

    @Override
    public void run() {
        while(!this.tokenBucket.terminated()) {
            if (maxBytesPerSecond == null) {
            	this.tokenBucket.set(Long.MAX_VALUE);
            } else {
            	this.tokenBucket.add(this.maxBytesPerSecond);
            }
        	try {
				Thread.sleep(1000); // adding maxBps to token bucket every second
			} catch (InterruptedException e) {
				System.err.println("The rate limiter was interruped");
			}
        }
    }
}
